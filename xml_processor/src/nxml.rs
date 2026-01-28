use anyhow::Result;
use polars::prelude::*;
use pyo3::prelude::*;
use pyo3_polars::PyDataFrame;
use quick_xml::events::Event;
use quick_xml::Reader;
use regex::Regex;
use serde::{Deserialize, Serialize};
use std::fs::File;
use std::io::Write;

/// Metadata for an article
#[derive(Serialize, Deserialize, Default)]
pub struct ArticleMetadata {
    pub pmid: Option<String>,
    pub pmc_id: Option<String>,
    pub title: Option<String>,
    pub abstract_text: Option<String>,
    pub authors: Vec<String>,
    pub journal: Option<String>,
    pub publication_date: Option<String>,
    pub doi: Option<String>,
    pub full_text: Option<String>,
    pub file_path: String,
}

/// Extract key metadata and text from PMC XML content
pub fn extract_article_metadata(xml_content: &str, file_path: &str) -> Result<ArticleMetadata> {
    let mut reader = Reader::from_str(xml_content);
    reader.config_mut().trim_text(true);

    let mut metadata = ArticleMetadata::default();
    metadata.file_path = file_path.to_string();

    let mut buf = Vec::new();
    let mut current_text = String::new();
    let mut in_title = false;
    let mut in_abstract = false;
    let mut in_contrib = false;
    let mut in_surname = false;
    let mut in_given_names = false;
    let mut in_journal = false;
    let mut in_body = false;
    let mut in_pmid = false;
    let mut in_pmc_id = false;
    let mut in_doi = false;
    let mut in_pub_date = false;
    let mut in_year = false;
    let mut in_month = false;
    let mut in_day = false;
    let mut full_text_parts = Vec::new();

    // For author extraction
    let mut current_surname = String::new();
    let mut current_given_names = String::new();

    // For publication date extraction
    let mut current_year = String::new();
    let mut current_month = String::new();
    let mut current_day = String::new();

    // Track document structure to avoid extracting from references/supplementary
    let mut in_front_matter = false;
    let mut title_extracted = false; // Only extract the first title

    loop {
        match reader.read_event_into(&mut buf) {
            Ok(Event::Start(ref e)) => {
                match e.name().as_ref() {
                    b"front" => {
                        in_front_matter = true;
                    }
                    b"back" => {
                        in_front_matter = false;
                    }
                    b"article-title" => {
                        // Only extract title if we're in front matter and haven't extracted one yet
                        if in_front_matter && !title_extracted {
                            in_title = true;
                            current_text.clear();
                        }
                    }
                    b"abstract" => {
                        if in_front_matter {
                            in_abstract = true;
                            current_text.clear();
                        }
                    }
                    b"contrib" => {
                        if in_front_matter {
                            // Check if this is an author contribution
                            for attr in e.attributes() {
                                if let Ok(attr) = attr {
                                    if attr.key.as_ref() == b"contrib-type" {
                                        let value = String::from_utf8_lossy(&attr.value);
                                        if value == "author" {
                                            in_contrib = true;
                                            current_surname.clear();
                                            current_given_names.clear();
                                        }
                                    }
                                }
                            }
                        }
                    }
                    b"surname" => {
                        if in_contrib && in_front_matter {
                            in_surname = true;
                            current_surname.clear();
                        }
                    }
                    b"given-names" => {
                        if in_contrib && in_front_matter {
                            in_given_names = true;
                            current_given_names.clear();
                        }
                    }
                    b"journal-title" => {
                        if in_front_matter {
                            in_journal = true;
                            current_text.clear();
                        }
                    }
                    b"pub-date" => {
                        if in_front_matter {
                            in_pub_date = true;
                            current_year.clear();
                            current_month.clear();
                            current_day.clear();
                        }
                    }
                    b"year" => {
                        if in_pub_date && in_front_matter {
                            in_year = true;
                            current_year.clear();
                        }
                    }
                    b"month" => {
                        if in_pub_date && in_front_matter {
                            in_month = true;
                            current_month.clear();
                        }
                    }
                    b"day" => {
                        if in_pub_date && in_front_matter {
                            in_day = true;
                            current_day.clear();
                        }
                    }
                    b"body" => {
                        in_body = true;
                    }
                    b"article-id" => {
                        if in_front_matter {
                            current_text.clear();
                            for attr in e.attributes() {
                                if let Ok(attr) = attr {
                                    if attr.key.as_ref() == b"pub-id-type" {
                                        let value = String::from_utf8_lossy(&attr.value);
                                        match value.as_ref() {
                                            "pmid" => in_pmid = true,
                                            "pmc" => in_pmc_id = true,
                                            "doi" => in_doi = true,
                                            _ => {}
                                        }
                                    }
                                }
                            }
                        }
                    }
                    _ => {}
                }
            }
            Ok(Event::Text(e)) => {
                let text = std::str::from_utf8(e.as_ref()).unwrap_or_default();

                // Handle author name components
                if in_surname && in_front_matter {
                    current_surname.push_str(text);
                } else if in_given_names && in_front_matter {
                    current_given_names.push_str(text);
                } else if in_year && in_pub_date && in_front_matter {
                    current_year.push_str(text);
                } else if in_month && in_pub_date && in_front_matter {
                    current_month.push_str(text);
                } else if in_day && in_pub_date && in_front_matter {
                    current_day.push_str(text);
                } else if (in_title || in_abstract || in_journal || in_pmid || in_pmc_id || in_doi)
                    && in_front_matter
                {
                    current_text.push_str(text);
                }

                if in_body {
                    full_text_parts.push(text.to_string());
                }
            }
            Ok(Event::End(ref e)) => {
                match e.name().as_ref() {
                    b"front" => {
                        in_front_matter = false;
                    }
                    b"article-title" => {
                        if in_title && in_front_matter && !title_extracted {
                            let trimmed = current_text.trim();
                            if !trimmed.is_empty() {
                                metadata.title = Some(trimmed.to_string());
                                title_extracted = true;
                            }
                            current_text.clear();
                            in_title = false;
                        }
                    }
                    b"abstract" => {
                        if in_abstract && in_front_matter {
                            let trimmed = current_text.trim();
                            if !trimmed.is_empty() {
                                metadata.abstract_text = Some(trimmed.to_string());
                            }
                            current_text.clear();
                            in_abstract = false;
                        }
                    }
                    b"contrib" => {
                        if in_contrib && in_front_matter {
                            // Construct author name from surname and given names
                            let surname = current_surname.trim();
                            let given_names = current_given_names.trim();

                            if !surname.is_empty() || !given_names.is_empty() {
                                let author_name = if !surname.is_empty() && !given_names.is_empty()
                                {
                                    format!("{surname}, {given_names}")
                                } else if !surname.is_empty() {
                                    surname.to_string()
                                } else {
                                    given_names.to_string()
                                };

                                metadata.authors.push(author_name);
                            }

                            in_contrib = false;
                            current_surname.clear();
                            current_given_names.clear();
                        }
                    }
                    b"surname" => {
                        in_surname = false;
                    }
                    b"given-names" => {
                        in_given_names = false;
                    }
                    b"journal-title" => {
                        if in_journal && in_front_matter {
                            let trimmed = current_text.trim();
                            if !trimmed.is_empty() {
                                metadata.journal = Some(trimmed.to_string());
                            }
                            current_text.clear();
                            in_journal = false;
                        }
                    }
                    b"pub-date" => {
                        if in_pub_date && in_front_matter {
                            // Construct publication date from year, month, day
                            let year = current_year.trim();
                            let month = current_month.trim();
                            let day = current_day.trim();

                            if !year.is_empty() {
                                let mut date_parts = vec![year];
                                if !month.is_empty() {
                                    // Convert month name to number if needed
                                    let month_num = match month.to_lowercase().as_str() {
                                        "january" | "jan" => "01",
                                        "february" | "feb" => "02",
                                        "march" | "mar" => "03",
                                        "april" | "apr" => "04",
                                        "may" => "05",
                                        "june" | "jun" => "06",
                                        "july" | "jul" => "07",
                                        "august" | "aug" => "08",
                                        "september" | "sep" => "09",
                                        "october" | "oct" => "10",
                                        "november" | "nov" => "11",
                                        "december" | "dec" => "12",
                                        _ => month, // Assume it's already a number
                                    };
                                    date_parts.push(month_num);

                                    if !day.is_empty() {
                                        date_parts.push(day);
                                    }
                                }

                                metadata.publication_date = Some(date_parts.join("-"));
                            }

                            in_pub_date = false;
                            current_year.clear();
                            current_month.clear();
                            current_day.clear();
                        }
                    }
                    b"year" => {
                        in_year = false;
                    }
                    b"month" => {
                        in_month = false;
                    }
                    b"day" => {
                        in_day = false;
                    }
                    b"article-id" => {
                        if in_front_matter {
                            let text_content = current_text.trim();
                            if in_pmid && !text_content.is_empty() {
                                metadata.pmid = Some(text_content.to_string());
                                in_pmid = false;
                            } else if in_pmc_id && !text_content.is_empty() {
                                if text_content.starts_with("PMC") {
                                    metadata.pmc_id = Some(text_content.to_string());
                                } else {
                                    metadata.pmc_id = Some(format!("PMC{text_content}"));
                                }
                                in_pmc_id = false;
                            } else if in_doi && !text_content.is_empty() {
                                metadata.doi = Some(text_content.to_string());
                                in_doi = false;
                            }
                            current_text.clear();
                        }
                    }
                    b"body" => {
                        in_body = false;
                    }
                    _ => {}
                }
            }
            Ok(Event::Eof) => break,
            Err(e) => return Err(anyhow::anyhow!("Error parsing XML: {}", e)),
            _ => {}
        }
        buf.clear();
    }

    if !full_text_parts.is_empty() {
        metadata.full_text = Some(full_text_parts.join(" "));
    }

    Ok(metadata)
}
/// Convert a single XML file to NDJSON format
#[pyfunction]
pub fn xml_to_ndjson(xml_path: &str, output_path: &str) -> PyResult<()> {
    let xml_content = std::fs::read_to_string(xml_path).map_err(|e| {
        PyErr::new::<pyo3::exceptions::PyIOError, _>(format!("Failed to read XML file: {e}"))
    })?;

    let metadata = extract_article_metadata(&xml_content, xml_path).map_err(|e| {
        PyErr::new::<pyo3::exceptions::PyValueError, _>(format!("Failed to extract metadata: {e}"))
    })?;

    let json_line = serde_json::to_string(&metadata).map_err(|e| {
        PyErr::new::<pyo3::exceptions::PyValueError, _>(format!("Failed to serialize to JSON: {e}"))
    })?;

    let mut output_file = File::create(output_path).map_err(|e| {
        PyErr::new::<pyo3::exceptions::PyIOError, _>(format!("Failed to create output file: {e}"))
    })?;

    writeln!(output_file, "{json_line}").map_err(|e| {
        PyErr::new::<pyo3::exceptions::PyIOError, _>(format!("Failed to write to output file: {e}"))
    })?;

    Ok(())
}

/// Convert multiple XML files to a single NDJSON file
#[pyfunction]
pub fn batch_xml_to_ndjson(
    py: Python, // <‑‑ new
    xml_paths: Vec<String>,
    output_path: &str,
) -> PyResult<usize> {
    let result: std::result::Result<usize, _> = py.allow_threads(|| {
        let mut output_file: File = File::create(output_path).map_err(|e: std::io::Error| {
            PyErr::new::<pyo3::exceptions::PyIOError, _>(format!(
                "Failed to create output file: {e}"
            ))
        })?;

        let mut processed_count = 0;

        for xml_path in &xml_paths {
            match std::fs::read_to_string(xml_path) {
                Ok(xml_content) => match extract_article_metadata(&xml_content, xml_path) {
                    Ok(metadata) => match serde_json::to_string(&metadata) {
                        Ok(json_line) => {
                            if writeln!(output_file, "{json_line}").is_ok() {
                                processed_count += 1;
                            }
                        }
                        Err(e) => eprintln!("Failed to serialize metadata for {xml_path}: {e}"),
                    },
                    Err(e) => eprintln!("Failed to extract metadata from {xml_path}: {e}"),
                },
                Err(e) => eprintln!("Failed to read {xml_path}: {e}"),
            }
        }

        Ok(processed_count)
    });

    // Explicit, **Send + Sync** error type so the closure satisfies the
    // `Ungil` requirement of `allow_threads`.
    result.map_err(|e: Box<dyn std::error::Error + Send + Sync>| {
        PyErr::new::<pyo3::exceptions::PyRuntimeError, _>(format!("{:?}", e))
    })
}

/// Read XML files (list of strings for paths) directly into a Polars DataFrame
#[pyfunction(signature = (xml_paths))]
pub fn xml_to_polars(py: Python, xml_paths: Vec<String>) -> PyResult<PyDataFrame> {
    let result = py.allow_threads(|| {
        let mut pmids = Vec::new();
        let mut pmc_ids = Vec::new();
        let mut titles = Vec::new();
        let mut abstracts = Vec::new();
        let mut journals = Vec::new();
        let mut full_texts = Vec::new();

        for xml_path in &xml_paths {
            match std::fs::read_to_string(xml_path) {
                Ok(xml_content) => {
                    match extract_article_metadata(&xml_content, xml_path) {
                        Ok(metadata) => {
                            pmids.push(metadata.pmid);
                            pmc_ids.push(metadata.pmc_id);
                            titles.push(metadata.title);
                            abstracts.push(metadata.abstract_text);
                            journals.push(metadata.journal);
                            full_texts.push(metadata.full_text);
                        }
                        Err(e) => {
                            eprintln!("Failed to extract metadata from {xml_path}: {e}");
                            // Add None values to maintain alignment
                            pmids.push(None);
                            pmc_ids.push(None);
                            titles.push(None);
                            abstracts.push(None);
                            journals.push(None);
                            full_texts.push(None);
                        }
                    }
                }
                Err(e) => {
                    eprintln!("Failed to read {xml_path}: {e}");
                    // Add None values to maintain alignment
                    pmids.push(None);
                    pmc_ids.push(None);
                    titles.push(None);
                    abstracts.push(None);
                    journals.push(None);
                    full_texts.push(None);
                }
            }
        }

        df! {
            "pmid" => &pmids,
            "pmc_id" => &pmc_ids,
            "title" => &titles,
            "abstract" => &abstracts,
            "journal" => &journals,
            "full_text" => &full_texts,
        }
    });

    let df = result.map_err(|e| {
        PyErr::new::<pyo3::exceptions::PyValueError, _>(format!("Failed to create DataFrame: {e}"))
    })?;

    Ok(PyDataFrame(df))
}

/// Search for patterns in XML content and return matching articles
#[pyfunction(signature = (xml_paths, patterns, case_sensitive=None))]
pub fn search_xml_content(
    xml_paths: Vec<String>,
    patterns: Vec<String>,
    case_sensitive: Option<bool>,
) -> PyResult<PyDataFrame> {
    let case_sensitive = case_sensitive.unwrap_or(false);
    let mut regex_patterns = Vec::new();

    // Compile regex patterns
    for pattern in &patterns {
        let regex = if case_sensitive {
            Regex::new(pattern)
        } else {
            Regex::new(&format!("(?i){pattern}"))
        }
        .map_err(|e| {
            PyErr::new::<pyo3::exceptions::PyValueError, _>(format!(
                "Invalid regex pattern '{pattern}': {e}"
            ))
        })?;

        regex_patterns.push(regex);
    }

    let mut matched_file_paths = Vec::new();
    let mut matched_patterns = Vec::new();
    let mut match_contexts = Vec::new();

    for xml_path in &xml_paths {
        if let Ok(xml_content) = std::fs::read_to_string(xml_path) {
            for (pattern_idx, regex) in regex_patterns.iter().enumerate() {
                for mat in regex.find_iter(&xml_content) {
                    matched_file_paths.push(Some(xml_path.to_string()));
                    matched_patterns.push(Some(patterns[pattern_idx].clone()));

                    // Extract context around the match (100 chars before and after)
                    let start = mat.start().saturating_sub(100);
                    let end = (mat.end() + 100).min(xml_content.len());
                    let context = xml_content[start..end].to_string();
                    match_contexts.push(Some(context));
                }
            }
        }
    }

    let df = df! {
        "file_path" => &matched_file_paths,
        "matched_pattern" => &matched_patterns,
        "match_context" => &match_contexts,
    }
    .map_err(|e| {
        PyErr::new::<pyo3::exceptions::PyValueError, _>(format!(
            "Failed to create search results DataFrame: {e}"
        ))
    })?;

    Ok(PyDataFrame(df))
}
