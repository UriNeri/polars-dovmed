use pyo3::prelude::*;
mod nxml;

#[pymodule]
fn xml_processor(py: Python, m: &Bound<'_, PyModule>) -> PyResult<()> {
    // Add nxml functions to a submodule
    let nxml_mod = PyModule::new(py, "nxml")?;
    nxml_mod.add_function(wrap_pyfunction!(nxml::xml_to_ndjson, py)?)?;
    nxml_mod.add_function(wrap_pyfunction!(nxml::batch_xml_to_ndjson, py)?)?;
    nxml_mod.add_function(wrap_pyfunction!(nxml::xml_to_polars, py)?)?;
    nxml_mod.add_function(wrap_pyfunction!(nxml::search_xml_content, py)?)?;

    // Add submodules to the main module
    m.add_submodule(&nxml_mod)?;

    // Register submodules in sys.modules for proper import
    py.import("sys")?
        .getattr("modules")?
        .set_item("xml_processor.nxml", nxml_mod)?;

    Ok(())
}
