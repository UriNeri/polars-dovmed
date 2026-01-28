"""Quick smoke tests to verify basic functionality."""

import subprocess
import sys


def test_import_polars_dovmed():
    """Test that polars_dovmed can be imported."""
    import polars_dovmed
    assert hasattr(polars_dovmed, "__version__")
    print(f"✓ polars_dovmed v{polars_dovmed.__version__}")


def test_import_xml_processor():
    """Test that xml_processor can be imported."""
    from polars_dovmed import xml_processor
    assert hasattr(xml_processor, "nxml")
    print("✓ xml_processor module loaded")


def test_cli_available():
    """Test that dovmed CLI is available."""
    result = subprocess.run(
        [sys.executable, "-m", "polars_dovmed.cli", "--help"],
        capture_output=True,
        text=True,
    )
    assert result.returncode == 0
    assert "dovmed" in result.stdout.lower()
    print("✓ CLI is available")


def test_cli_commands_listed():
    """Test that all main commands are listed in help."""
    result = subprocess.run(
        [sys.executable, "-m", "polars_dovmed.cli", "--help"],
        capture_output=True,
        text=True,
    )
    
    commands = ["scan", "download", "build-parquet", "create-patterns"]
    for cmd in commands:
        assert cmd in result.stdout, f"Command '{cmd}' not found in help"
    
    print(f"✓ All commands present: {', '.join(commands)}")


def test_xml_processor_functions():
    """Test that xml_processor has expected functions."""
    from polars_dovmed import xml_processor
    
    # Check for nxml submodule functions
    assert hasattr(xml_processor.nxml, "xml_to_polars")
    assert hasattr(xml_processor.nxml, "xml_to_ndjson")
    assert hasattr(xml_processor.nxml, "batch_xml_to_ndjson")
    assert hasattr(xml_processor.nxml, "search_xml_content")
    
    print("✓ XML processor functions available")
