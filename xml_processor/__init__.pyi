"""Stub file for xml_processor module providing type annotations."""

from typing import List, Optional
from polars import DataFrame

class nxml:
    """NXML processing submodule for PMC XML files."""
    
    @staticmethod
    def xml_to_ndjson(xml_path: str, output_path: str) -> None:
        """
        Convert a single XML file to NDJSON format.
        
        Args:
            xml_path: Path to the input XML file
            output_path: Path where the output NDJSON file will be written
            
        Raises:
            IOError: If the XML file cannot be read or output file cannot be written
            ValueError: If the XML content cannot be parsed or serialized
        """
        ...
    
    @staticmethod
    def batch_xml_to_ndjson(xml_paths: List[str], output_path: str) -> int:
        """
        Convert multiple XML files to a single NDJSON file.
        
        Args:
            xml_paths: List of paths to XML files to process
            output_path: Path where the output NDJSON file will be written
            
        Returns:
            Number of files successfully processed
            
        Raises:
            IOError: If the output file cannot be created
        """
        ...
    
    @staticmethod
    def xml_to_polars(xml_paths: List[str]) -> DataFrame:
        """
        Read XML files directly into a Polars DataFrame.
        
        Args:
            xml_paths: List of paths to XML files to process
            
        Returns:
            Polars DataFrame with columns:
            - pmid: Optional[str] - PubMed ID
            - pmc_id: Optional[str] - PMC ID  
            - title: Optional[str] - Article title
            - abstract: Optional[str] - Abstract text
            - journal: Optional[str] - Journal name
            - full_text: Optional[str] - Full article text
            
        Raises:
            ValueError: If DataFrame creation fails
        """
        ...
    
    @staticmethod
    def search_xml_content(
        xml_paths: List[str],
        patterns: List[str], 
        case_sensitive: Optional[bool] = None
    ) -> DataFrame:
        """
        Search for patterns in XML content and return matching articles.
        
        Args:
            xml_paths: List of paths to XML files to search
            patterns: List of regex patterns to search for
            case_sensitive: Whether search should be case sensitive (default: False)
            
        Returns:
            Polars DataFrame with columns:
            - file_path: Optional[str] - Path to file containing match
            - matched_pattern: Optional[str] - Pattern that matched
            - match_context: Optional[str] - Context around the match (±100 chars)
            
        Raises:
            ValueError: If regex patterns are invalid
        """
        ...