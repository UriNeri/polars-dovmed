"""
LLM examination of retrieved literature, and conversion to standard format IF relevant.
"""

import argparse
import json
import logging
import os
import re
import sys
from pathlib import Path
from typing import Dict

import polars as pl
from dotenv import load_dotenv

from polars_dovmed.llm_utils import (
    call_llm_api,
    list_available_models,
    normalize_model_name,
)
from polars_dovmed.schema_utils import (
    generate_biological_response_schema,
    normalize_biological_name,
    save_schema,
    validate_response,
)
from polars_dovmed.utils import (
    ChunkProgressReporter,
    clean_pattern_for_polars,
    setup_logging,
)

# Load environment variables
load_dotenv()

# Initialize logger
logger = logging.getLogger(__name__)

# Schema generation is now handled by schema_utils module


def create_system_prompt_full_text(schema: Dict = None) -> str:  # type: ignore
    """Create the system prompt for full-text LLM analysis.

    Args:
        schema: JSON schema to include in the prompt for response validation
    """
    base_prompt = """You are an expert bioinformatics researcher analyzing complete scientific papers. Your task is to examine the full text of a research paper and respond with structured information in JSON format.

You will be given:
1. A complete scientific paper text
2. A target biological concept type (e.g. molecular function, domain, gene, protein, variant, organism life style or phenomena)
3. A specific term that was detected in the paper based on string matching
4. A list of terms related to the biological concept (note, these may be in a regex format used in the string search)

CONTROLLED VOCABULARIES AND EXTRACTION GUIDELINES:

1. NAME ATTRIBUTE:
   Preferred formats:
   - Specific biological feature (e.g., "Frameshifting element", "5' UTR")
3. If the user's interest is in fungal orthologues of some bacterial genes, and the text includes "... a similar function exists in certain fungi, such as Aspergillus nidulans protein Xyz..."
4. If the user's interest is about a parasite life style/cycle, and the text includes "... early stages of parasitus maximus occur in birds, often corvids...".
5. If the user's interest is in a specific molecular function, and the full text includes: "... the reaction (substrate) is oxidized by enzyme Xyz to produce product abc...".
6. If the user's interest is in non-coding RNA in a specific organism, and the text include: "... a non-coding region is transcribed on top (in overlap) of gene bla1, nested between position 100 and 200..."

ANALYSIS APPROCH:
- Read through the entire paper.
- Look for all mentions of the target term and related biological entities
- Search for actionable information (genomic coordinates, protein positions, sequence accessions, database names, and database identifiers).
- Evaluate whether the concept and the actionable information are related (e.g. genomic coordinates refer to the same biological entity where the phenomena of interest is).
- Consider the mentioned context - introduction/background sections (such as litrature overviews) may refer to different entities than those discussed in the results/methods.

CRITICAL EVALUATION CRITERIA:
- False positives are common - the input is only loosely filtered based on string searches, so the detected term may refer to unrelated concepts (e.g., acronym disambiguation).
- Synthetic constructs, expression plasmids, modified sequences, antibodies, vaccines, artifical vectors, and biosensors should generally be marked as not relevant, as they do not describe the naturally occuring phenomena/concept.
- The actioanable information may be scattered across the paper: for example the accessions and database may be mentioned in a "data availability" section, but the coordinates could be in the results, methods or supplementary sections.

RESPONSE FORMAT:
If there is actionable information regarding the concept, extract and consolidate it.
Your response must ONLY contain a valid JSON object with one of these structures:

If the paper is relevant to the concept and you found most of the required/actionable information (database, identifier, coordinates):
{
    "is_relevant": "relevant", 
    "reason": "brief summary of why the manuscript is relevant",
    "coordinate_list": [
        {
            "name": "the user's term or biological concept of interest this item is about",
            "type": "RNA, DNA, Protein (amino acid)",
            "organism": "specific taxid/taxon_id if mentioned, if not then species/organism name (if not mentioned, leave empty)",
            "database": "source database for identifiers (GenBank, UniProt, IMG/M etc.)",
            "accession": "unique accession/identifier if available",
            "start": "start position if available",
            "end": "end position if available", 
            "strand": (For nucleic acids) "1" (forward) or "-1" (reverse). if missing or if the item is a protein, set to "",
            "sequence": "specific nucleic acid or amino acid sequences if provided"
        }
    ]
}

If the paper is relevant to the concept, but not enough actionable information is available in the text, set "is_relevant" to "insufficient".
For example, if actionable data (coordinates, concept, organism, identifiers) are not entirely mentioned or if they are noted as being available elsewhere (e.g., "...see supplementary material").
{
    "is_relevant": "insufficient",
    "reason": "what information is missing and where it might be found",
    "coordinate_list": []
}

If the term is not relevant to the target concept, or is mentioned in relation to synthetic/artificial constructs:
{
    "is_relevant": "not_relevant",
    "reason": "concise explanation of why the term/information is not relevant",
    "coordinate_list": []
}

EXAMPLE VALID RESPONSE:
{
    "is_relevant": "relevant",
    "reason": "Detailed characterization of a Frameshifting sequence in a specific virus",
    "coordinate_list": [
        {
            "name": "Frameshifting element",
            "type": "RNA",
            "organism": "Citrus yellow vein-associated virus",
            "database": "GenBank",
            "accession": "JX101610",
            "start": "2399",
            "end": "668",
            "strand": "1",
            "sequence": "GGUUUAAU"
        }
    ]
}

IMPORTANT NOTES:
- Only extract information explicitly stated in the paper.
- Do not convert gene names to accessions unless explicitly provided.
- Ensure the term and coordinates refer to the same biological entity.
- If uncertain prefer "insufficient" over "relevant".
- There may be multiple actionable information in the paper relating to the concept of interest - list all of them in the coordinate_list attribute.
- For missing values, use an empty string ("") - do not use "Nan" or "N/A" or "null" or "MISSING".
- ONLY RESPOND WITH A VALID JSON: without comments or text outside of the JSON. String values (even if empty) MUST be enclosed in double quotes. The last item in a list/array should not be followed by a comma.
- All numeric values for (for positions or for strand) should be double qouted as strings (e.g. "1" or "-1" for strand), not integers.
"""

    # Add schema information if provided
    if schema:
        schema_prompt = f"""

IMPORTANT: Your response MUST strictly conform to the following JSON schema:

{json.dumps(schema, indent=2)}

Key constraints from the schema:
- "is_relevant" must be one of: {schema["properties"]["is_relevant"]["enum"]}
- "type" must be one of: {schema["properties"]["coordinate_list"]["items"]["properties"]["type"]["enum"]}
- "strand" must be one of: {schema["properties"]["coordinate_list"]["items"]["properties"]["strand"]["enum"]}
- "database" must be one of the predefined database names in the schema
- "name" should match one of the predefined biological concepts in the schema
- All string fields must use empty string ("") for missing values, never null or undefined
- All position values (start, end) must be strings, not numbers

Do not deviate from these controlled vocabularies. If a value doesn't fit the schema options, choose the closest match or use an empty string.
"""
        return base_prompt + schema_prompt

    return base_prompt


def create_user_prompt_full_text(
    full_text: str,
    user_terms: str | Dict,
    matched_terms: str,
    title: str,
    prompt_prepend: str | None = None,
    prompt_append: str | None = None,
) -> str:
    """Create the user prompt with the full text of the paper to be analyzed."""
    prompt = "Analyze the following full scientific paper text and return the appropriate JSON response:\n\n"

    # Add prepend text if provided
    if prompt_prepend and prompt_prepend.strip():
        prompt += f"The user also notes: {prompt_prepend.strip()}\n\n"

    prompt += f"Paper title: {title}\n\n"
    prompt += f"All terms the user is interested in : {user_terms}\n"
    prompt += (
        f"The specific texts that were matched in this paper: '{matched_terms}'\n\n"
    )
    prompt += f"Full paper text:\n{full_text}\n\n"

    # Add append text if provided
    if prompt_append and prompt_append.strip():
        prompt += f"\n\nThe user also notes: {prompt_append.strip()}"

    return prompt


def fix_common_json_issues(text: str) -> str:
    """
    Comprehensive JSON fixing function with multiple strategies
    """
    # Remove any leading/trailing whitespace
    text = text.strip()

    # Ensure proper JSON structure
    if not text.startswith("{"):
        text = "{" + text
    if not text.endswith("}"):
        text += "}"

    # Fix unescaped special characters in strings
    text = re.sub(r'(?<!\\)(["\\/\b\f\n\r\t])', r"\\\1", text)

    # Normalize quotes
    text = text.replace("'", '"')

    # Fix missing quotes around keys
    text = re.sub(r"(\w+)(\s*:)", r'"\1"\2', text)

    # Handle numeric and special values
    text = re.sub(r"(\d+)\s*(kb|Kb|KB)\s*apart", r'"\1 kb apart"', text)

    # Remove trailing commas
    text = re.sub(r",\s*}", "}", text)
    text = re.sub(r",\s*\]", "]", text)

    return text


def validate_response_against_schema(response: Dict, schema: Dict) -> Dict:
    """Validate and clean response against schema constraints.

    Args:
        response: Parsed JSON response from LLM
        schema: JSON schema for validation

    Returns:
        Cleaned and validated response
    """
    try:
        # Validate is_relevant
        valid_relevance = schema["properties"]["is_relevant"]["enum"]
        if response.get("is_relevant") not in valid_relevance:
            logger.warning(
                f"Invalid is_relevant value: {response.get('is_relevant')}. Setting to 'parsing_error'"
            )
            response["is_relevant"] = "parsing_error"

        # Validate coordinate_list items
        if "coordinate_list" in response and isinstance(
            response["coordinate_list"], list
        ):
            coord_schema = schema["properties"]["coordinate_list"]["items"][
                "properties"
            ]

            for i, coord in enumerate(response["coordinate_list"]):
                # Validate type
                valid_types = coord_schema["type"]["enum"]
                if coord.get("type") not in valid_types:
                    # Try to map common variations
                    type_mapping = {
                        "protein": "Protein",
                        "rna": "RNA",
                        "dna": "DNA",
                        "rna_structure": "RNA",
                        "stem_loop": "RNA",
                        "nucleotide": "DNA",
                    }
                    mapped_type = type_mapping.get(coord.get("type", "").lower())
                    if mapped_type:
                        coord["type"] = mapped_type
                        logger.info(
                            f"Mapped type '{coord.get('type')}' to '{mapped_type}'"
                        )
                    else:
                        coord["type"] = "RNA"  # Default fallback
                        logger.warning(
                            f"Invalid type '{coord.get('type')}' in coordinate {i}. Setting to 'RNA'"
                        )

                # Validate strand
                valid_strands = coord_schema["strand"]["enum"]
                if coord.get("strand") not in valid_strands:
                    coord["strand"] = ""
                    logger.warning(
                        f"Invalid strand value in coordinate {i}. Setting to empty string"
                    )

                # Validate database
                valid_databases = coord_schema["database"]["enum"]
                if coord.get("database") not in valid_databases:
                    # Try to map common variations
                    db_mapping = {
                        "genbank": "ncbi_genbank",
                        "refseq": "ncbi_refseq",
                        "protein_data_bank": "PDB",
                        "ncbi_virus_refseq": "ncbi_refseq",
                    }
                    mapped_db = db_mapping.get(coord.get("database", "").lower())
                    if mapped_db:
                        coord["database"] = mapped_db
                        logger.info(
                            f"Mapped database '{coord.get('database')}' to '{mapped_db}'"
                        )
                    else:
                        coord["database"] = ""
                        logger.warning(
                            f"Invalid database '{coord.get('database')}' in coordinate {i}. Setting to empty string"
                        )

                # Normalize name field to lowercase with underscores
                if "name" in coord and coord["name"]:
                    coord["name"] = normalize_biological_name(coord["name"])

                # Ensure all required fields are present with empty strings if missing
                required_fields = coord_schema.keys()
                for field in required_fields:
                    if field not in coord:
                        coord[field] = ""
                    elif coord[field] is None:
                        coord[field] = ""
                    elif not isinstance(coord[field], str):
                        coord[field] = str(coord[field])

        return response

    except Exception as e:
        logger.error(f"Schema validation failed: {e}")
        return response


def parse_llm_response(response_text: str, schema: Dict = None) -> Dict:  # type: ignore
    """
    Robust LLM response parsing with comprehensive error handling and schema validation

    Args:
        response_text: Raw response text from LLM
        schema: Optional JSON schema for validation
    """
    response_text = response_text.strip()

    # Remove markdown code blocks
    for prefix in ["```json", "```"]:
        if response_text.startswith(prefix):
            response_text = response_text[len(prefix) :]
    if response_text.endswith("```"):
        response_text = response_text[:-3]

    # Attempts to parse with progressive fixes
    attempts = [
        response_text,  # Original
        fix_common_json_issues(response_text),  # Comprehensive fix
        fix_common_json_issues(response_text.replace("\n", " ")),  # Remove newlines
    ]

    for i, attempt in enumerate(attempts):
        try:
            # Validate JSON structure
            response = json.loads(attempt)

            # Validate required keys
            required_keys = {"is_relevant", "reason", "coordinate_list"}
            if not all(key in response for key in required_keys):
                raise ValueError("Missing required JSON keys")

            # Validate coordinate_list structure
            if not isinstance(response.get("coordinate_list", []), list):
                raise ValueError("coordinate_list must be a list")

            # Validate coordinate entries
            for coord in response.get("coordinate_list", []):
                coord_keys = {
                    "name",
                    "type",
                    "organism",
                    "database",
                    "accession",
                    "start",
                    "end",
                    "strand",
                    "sequence",
                }
                if not all(key in coord for key in coord_keys):
                    raise ValueError(f"Incomplete coordinate entry: {coord}")

            if i > 0:
                logger.warning(f"JSON parsed successfully after {i} fix attempt(s)")

            # Apply schema validation if schema is provided
            if schema:
                response = validate_response_against_schema(response, schema)

                # Also run formal JSON schema validation for logging
                is_valid, error_msg = validate_response(response, schema)
                if not is_valid:
                    logger.warning(f"Response failed schema validation: {error_msg}")

            return response

        except (json.JSONDecodeError, ValueError) as e:
            if i == len(attempts) - 1:  # Last attempt
                logger.error(f"Failed to parse JSON response: {e}")
                logger.error(f"Original response text: {response_text}")

                # Fallback to partial extraction
                try:
                    return extract_partial_json(response_text)
                except Exception:
                    return {
                        "is_relevant": "parsing_error",
                        "reason": f"JSON parsing failed: {str(e)}",
                        "coordinate_list": [],
                    }
            else:
                logger.debug(f"JSON parse attempt {i + 1} failed: {e}")
                continue


def extract_partial_json(text: str) -> Dict:
    """Attempt to extract partial JSON information when parsing fails completely."""
    # This is a fallback for when JSON is completely malformed
    # Try to extract key information using regex

    result = {
        "is_relevant": "parsing_error",
        "reason": "Failed to parse LLM response",
        "coordinate_list": [],
    }

    # Try to extract is_relevant field
    relevance_match = re.search(r'"is_relevant"\s*:\s*"([^"]*)"', text)
    if relevance_match:
        result["is_relevant"] = relevance_match.group(1)

    # Try to extract reason field
    reason_match = re.search(r'"reason"\s*:\s*"([^"]*)"', text)
    if reason_match:
        result["reason"] = reason_match.group(1)

    logger.warning("Extracted partial information from malformed JSON")
    return result


def main():
    """Main function to process literature contexts using LLM."""
    parser = argparse.ArgumentParser(
        description="Process literature contexts using LLM to extract biological coordinates",
        formatter_class=argparse.RawDescriptionHelpFormatter,
        epilog="""
Example usage:
  python src/polars_dovmed/llm_convert_context_to_coord.py \
    --input-df "results/pubmed_central/processed_literature_test/filtered.parquet" \
    --input-queries "RNA_virus_rss_queries.json" \
    --output-file "results/rna_virus/rna_secondary_structure/llm_full_text_responses/results.parquet" \
    --model "Llama-4-Scout-17B-16E-Instruct" \
    --api-base "https://api.openai.com/v1" \
    --api-key $LLM_API_KEY \
    --prompt-prepend "My focus is on RNA viruses and RNA secondary structures. I am NOT interested in antibodies, vaccines, artificial vectors, synthetic constructs or biosensors." \
    --schema-output "results/rna_virus/rna_secondary_structure/response_schema.json" \
    --additional-databases "Rfam" "VirDB" \
    --verbose
""",
    )
    parser.add_argument(
        "--input-df",
        required=True,
        help="Path to parquet file containing the processed dataframe.",
    )
    parser.add_argument(
        "--input-queries",
        required=True,
        help="Path to the original json file containing the queries (required for schema generation).",
    )
    parser.add_argument(
        "--output-file", required=True, help="Path to save the output parquet file"
    )
    parser.add_argument(
        "--model",
        default="Llama-4-Scout-17B-16E-Instruct",
        help="LLM model name to use",
    )
    parser.add_argument(
        "--api-base",
        required=True,
        help="Base URL for the LLM API (e.g., https://api.openai.com/v1)",
    )
    parser.add_argument(
        "--api-key",
        default=os.environ.get("LLM_API_KEY"),
        help="API key for authentication (can also use LLM_API_KEY env var)",
    )
    parser.add_argument(
        "--max-tokens", type=int, default=10000, help="Maximum tokens in LLM response"
    )
    parser.add_argument(
        "--temperature", type=float, default=0.1, help="Temperature for LLM generation"
    )
    parser.add_argument(
        "--prompt-prepend",
        help="Optional text to prepend to user prompt with 'The user also notes:' preface",
    )
    parser.add_argument(
        "--prompt-append",
        help="Optional text to append to user prompt with 'The user also notes:' preface",
    )
    parser.add_argument(
        "--schema-output", help="Path to save the generated JSON schema (optional)"
    )
    parser.add_argument(
        "--additional-databases",
        nargs="*",
        help="Additional database names to include in schema",
    )
    parser.add_argument(
        "--log-file",
        default="logs/llm_convert_context.log",
        help="Path to save the log",
    )
    parser.add_argument(
        "--verbose", "-v", action="store_true", help="Enable verbose logging"
    )

    args = parser.parse_args()

    # Setup logging
    setup_logging(log_file=args.log_file, verbose=args.verbose)
    Path(args.output_file).parent.mkdir(parents=True, exist_ok=True)

    try:
        logger.info("Testing API connection...")
        logger.info(f"Model: {args.model}")
        logger.info(f"API base: {args.api_base}")

        # Get available models and normalize model name
        available_models = list_available_models(args.api_base, args.api_key)
        normalized_model = normalize_model_name(args.model, available_models)

        if normalized_model != args.model:
            logger.info(f"Using normalized model name: {normalized_model}")

    except Exception as e:
        logger.error(f"❌ Failed to fetch models: {e}")
        sys.exit(1)

    # Load input dataframe
    input_df = pl.read_parquet(args.input_df)
    logger.info(f"Loaded input dataframe with {len(input_df)} rows")

    # Load the original queries if provided
    if args.input_queries:
        with open(args.input_queries, "r") as f:
            queries = json.load(f)
        logger.info("Loaded queries:")
        for key, value in queries.items():
            if key not in ["virus_taxonomy_report", "disqualifying_terms"]:
                logger.info(f"  {key}: {len(value)} patterns")
    else:
        queries = {}
        logger.warning(
            "No input queries provided - schema will use generic biological terms"
        )

    # Filter the dataframe
    logger.info("Filtering input dataframe...")

    # Basic quality filters
    quality_filters = [
        # Remove obviously irrelevant papers
        ~pl.col("title")
        .str.to_lowercase()
        .str.contains_any(
            ["retracted", "abstracts of", "congress", "poster", "abstracts", "oral"]
        ),
        # Only papers with matches
        pl.col("total_matches").ge(1),
        # Remove extremely long papers (likely OCR errors or concatenated documents)
        pl.col("full_text").str.len_chars().le(100000),
    ]

    filtered_df = input_df.filter(quality_filters)

    logger.info(
        f"Filtered dataframe has {len(filtered_df)} rows (from {len(input_df)} original)"
    )
    del input_df
    # Get concept columns

    # Create new dictionary for the loop.
    all_terms = queries.copy()

    # Remove the unwanted entries
    for key in ["virus_taxonomy_report", "disqualifying_terms"]:
        all_terms.pop(key, None)

    for key, value in all_terms.items():
        cleaned_patterns = []
        for pattern_list in value:
            cleaned_pattern_list = [
                clean_pattern_for_polars(pattern) for pattern in pattern_list
            ]
            cleaned_patterns.append(cleaned_pattern_list)
        all_terms[key] = cleaned_patterns

    print(all_terms)

    concept_columns = filtered_df.select(
        pl.selectors.starts_with(queries.keys())
    ).columns  # type: ignore
    logger.info(f"Found concept columns: {concept_columns}")

    # Generate response schema using the loaded queries
    logger.info("Generating response schema...")
    schema = generate_biological_response_schema(
        user_terms=queries,  # Use original queries dict, not all_terms
        additional_databases=args.additional_databases,
    )

    # Save schema if output path provided
    if args.schema_output:
        save_schema(schema, args.schema_output)
    else:
        # Save schema alongside results by default
        schema_path = Path(args.output_file).parent / "response_schema.json"
        save_schema(schema, str(schema_path))

    logger.info(
        f"Schema includes {len(schema['properties']['coordinate_list']['items']['properties']['name']['enum'])} name options"
    )
    logger.info(
        f"Schema includes {len(schema['properties']['coordinate_list']['items']['properties']['database']['enum'])} database options"
    )

    models = [args.model]
    all_responses = []

    # Create the system prompt with schema
    system_prompt = create_system_prompt_full_text(schema=schema)
    logger.debug(f"The system prompt is: {system_prompt}")

    # breakpoint()
    with ChunkProgressReporter(
        total_chunks=len(filtered_df),
        description="Processing matches with LLM",
        logger=logger,
        log_interval=5,
    ) as progress:
        for index, row in enumerate(filtered_df.iter_rows(named=True)):
            this_row_reponses = dict.fromkeys(models, None)

            user_prompt = create_user_prompt_full_text(
                full_text=row["full_text"],  # Use full text instead of coordinate_text
                title=row["title"],
                user_terms=all_terms,
                matched_terms=filtered_df[index]
                .select(concept_columns)
                .with_columns(pl.concat_list(pl.col(concept_columns)))
                .unique()
                .to_series()
                .to_list(),  # type: ignore
                prompt_prepend=args.prompt_prepend,
            )
            for model in models:
                try:
                    llm_response = call_llm_api(
                        user_prompt=user_prompt,
                        system_prompt=system_prompt,
                        api_key=args.api_key,
                        api_base=args.api_base,
                        model=model,
                    )
                    parsed = parse_llm_response(llm_response, schema=schema)

                except Exception as e:
                    parsed = {
                        "is_relevant": "ERROR",
                        "reason": str(e),
                        "coordinate_list": [],
                    }

                this_row_reponses[model] = parsed
                with open(
                    f"results/rna_virus/rna_secondary_structure/llm_full_text_respones/{row['pmc_id']}.json",
                    "w",
                ) as outfile:
                    json.dump(parsed, outfile)
            all_responses.append(this_row_reponses)
            logger.debug(this_row_reponses)
            progress.update()

    with open(
        "results/rna_virus/rna_secondary_structure/llm_full_text_respones/all_responses.jsonl",
        "w",
    ) as outfile:
        for response in all_responses:
            json.dump(response, outfile)
            outfile.write("\n")

    results_df = pl.from_dicts(all_responses, infer_schema_length=None)

    # Log summary statistics
    logger.info("\n=== LLM Processing Summary ===")
    logger.info(f"Total papers processed: {len(all_responses)}")

    # Count relevance categories across all models
    relevance_counts = {}
    for response_dict in all_responses:
        for model, response in response_dict.items():
            if response and "is_relevant" in response:
                relevance = response["is_relevant"]
                relevance_counts[relevance] = relevance_counts.get(relevance, 0) + 1

    logger.info("Relevance distribution:")
    for relevance, count in sorted(relevance_counts.items()):
        logger.info(f"  {relevance}: {count}")

    # Count coordinate extractions
    total_coordinates = 0
    for response_dict in all_responses:
        for model, response in response_dict.items():
            if response and "coordinate_list" in response:
                total_coordinates += len(response["coordinate_list"])

    logger.info(f"Total coordinates extracted: {total_coordinates}")
    logger.info("=" * 30)

    # Save the full results
    results_df.write_parquet(args.output_file)
    logger.info(f"Results saved to: {args.output_file}")


if __name__ == "__main__":
    main()
