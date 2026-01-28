"""
Utilities for generating and validating JSON schemas for LLM responses.
This module can be used independently to generate schemas for different projects.
"""

import json
import logging
from pathlib import Path
from typing import Any, Dict, List, Optional, Tuple

from jsonschema import ValidationError, validate

logger = logging.getLogger(__name__)


def normalize_biological_name(name: str) -> str:
    """Normalize biological names to lowercase with underscores.

    Args:
        name: The biological name to normalize

    Returns:
        Normalized name with lowercase letters and underscores instead of spaces/dashes
    """
    if not name or not isinstance(name, str):
        return ""
    return name.lower().replace(" ", "_").replace("-", "_")


def generate_biological_response_schema(
    user_terms: Optional[Dict] = None,
    additional_databases: Optional[List[str]] = None,
    additional_name_terms: Optional[List[str]] = None,
    include_common_organisms: bool = True,
) -> Dict[str, Any]:
    """
    Generate a comprehensive JSON schema for biological coordinate extraction responses.

    Args:
        user_terms: Dictionary of user terms/concepts from query files to include in name enum
        additional_databases: Additional database names to include in database enum
        additional_name_terms: Additional biological terms to include in name enum
        include_common_organisms: Whether to include common organism suggestions

    Returns:
        JSON schema dictionary
    """

    # Base databases from common biological databases
    base_databases = [
        "ncbi_genbank",
        "ncbi_refseq",
        "uniprot",
        "EMBL",
        "DDBJ",
        "PDB",
        "rfam",
        "ensembl",
        "IMG/M",
        "protein_data_bank",
        "",  # Empty string for missing values
    ]

    # Add additional databases if provided
    if additional_databases:
        base_databases.extend(additional_databases)

    # Remove duplicates and sort (keep empty string at end)
    database_enum = sorted([db for db in set(base_databases) if db != ""])
    database_enum.append("")  # Add empty string at end

    # Generate name enum based on user terms
    name_enum = []

    # Add terms derived from user_terms (query file concepts)
    if user_terms:
        for concept_type, patterns in user_terms.items():
            # Skip special entries that aren't biological concepts
            if concept_type in ["virus_taxonomy_report", "disqualifying_terms"]:
                continue

            # Add the concept type itself (cleaned up)
            clean_concept = normalize_biological_name(concept_type)
            name_enum.append(clean_concept)

    # Add additional name terms if provided
    if additional_name_terms:
        # Convert additional terms to lowercase with underscores
        cleaned_additional = [
            normalize_biological_name(term) for term in additional_name_terms
        ]
        name_enum.extend(cleaned_additional)

    # Add basic biological terms if no specific terms provided
    if not name_enum:
        name_enum = [
            "rna_structure",
            "rna_element",
            "protein_domain",
            "gene",
            "dna_element",
            "regulatory_sequence",
            "enzyme",
            "binding_site",
        ]

    # Remove duplicates and sort
    name_enum = sorted(list(set(name_enum)))

    # Common organism suggestions (optional)
    organism_examples = []
    if include_common_organisms:
        organism_examples = [
            "Homo sapiens",
            "Mus musculus",
            "Escherichia coli",
            "Saccharomyces cerevisiae",
            "Drosophila melanogaster",
            "Caenorhabditis elegans",
            "Arabidopsis thaliana",
        ]

    schema = {
        "$schema": "http://json-schema.org/draft-07/schema#",
        "title": "Biological Coordinate Extraction Response",
        "description": "Schema for LLM responses extracting biological coordinates and metadata",
        "type": "object",
        "properties": {
            "is_relevant": {
                "type": "string",
                "enum": ["relevant", "insufficient", "not_relevant", "parsing_error"],
                "description": "Indicates the relevance of the manuscript to the biological concept",
            },
            "reason": {
                "type": "string",
                "description": "Brief summary explaining the relevance determination",
                "minLength": 1,
            },
            "coordinate_list": {
                "type": "array",
                "description": "List of coordinate information for biological entities",
                "items": {
                    "type": "object",
                    "properties": {
                        "name": {
                            "type": "string",
                            "enum": name_enum,
                            "description": f"The biological concept of interest. Must be one of: {', '.join(name_enum[:5])}",
                        },
                        "type": {
                            "type": "string",
                            "enum": ["RNA", "DNA", "Protein"],
                            "description": "Type of biological molecule",
                        },
                        "organism": {
                            "type": "string",
                            "description": f"Taxid/taxon_id or virus/species/organism name. Examples: {', '.join(organism_examples[:3])}... Empty string if not mentioned.",
                        },
                        "database": {
                            "type": "string",
                            "enum": database_enum,
                            "description": f"Source database for identifiers. Must be one of: {', '.join([db for db in database_enum[:5] if db])}...",
                        },
                        "accession": {
                            "type": "string",
                            "description": "Unique accession/identifier (e.g., NM_001234, P12345, PF00001). Empty string if not available.",
                        },
                        "start": {
                            "type": "string",
                            "pattern": "^(|[0-9]+)$",
                            "description": "Start position as string (e.g., '100'). Empty string if not available.",
                        },
                        "end": {
                            "type": "string",
                            "pattern": "^(|[0-9]+)$",
                            "description": "End position as string (e.g., '200'). Empty string if not available.",
                        },
                        "strand": {
                            "type": "string",
                            "enum": ["1", "-1", ""],
                            "description": "Strand orientation for nucleic acids. '1' for forward/positive, '-1' for reverse/negative. Empty string for proteins or when not specified.",
                        },
                        "sequence": {
                            "type": "string",
                            "description": "Specific nucleic acid or amino acid sequence. Empty string if not provided.",
                        },
                    },
                    "required": [
                        "name",
                        "type",
                        "organism",
                        "database",
                        "accession",
                        "start",
                        "end",
                        "strand",
                        "sequence",
                    ],
                    "additionalProperties": False,
                },
            },
        },
        "required": ["is_relevant", "reason", "coordinate_list"],
        "additionalProperties": False,
    }

    return schema


def validate_response(
    response: Dict[str, Any], schema: Dict[str, Any]
) -> Tuple[bool, Optional[str]]:
    """
    Validate a response against the schema.

    Args:
        response: The response dictionary to validate
        schema: The JSON schema to validate against

    Returns:
        Tuple of (is_valid, error_message)
    """
    try:
        validate(instance=response, schema=schema)
        return True, None
    except ValidationError as e:
        return False, str(e)
    except Exception as e:
        return False, f"Validation error: {str(e)}"


def save_schema(schema: Dict[str, Any], output_path: str) -> None:
    """Save the JSON schema to a file with metadata."""
    Path(output_path).parent.mkdir(parents=True, exist_ok=True)

    # Add metadata
    schema_with_metadata = {
        "_metadata": {
            "generated_by": "polars_dovmed.schema_utils",
            "version": "1.0",
            "description": "JSON schema for biological coordinate extraction from literature",
        },
        **schema,
    }

    with open(output_path, "w") as f:
        json.dump(schema_with_metadata, f, indent=2)

    logger.info(f"Schema saved to: {output_path}")


def load_schema(schema_path: str) -> Dict[str, Any]:
    """Load a JSON schema from file."""
    with open(schema_path, "r") as f:
        schema_data = json.load(f)

    # Remove metadata if present
    if "_metadata" in schema_data:
        schema_data.pop("_metadata")

    return schema_data


if __name__ == "__main__":
    import argparse

    parser = argparse.ArgumentParser(
        description="Generate JSON schema for biological coordinate extraction"
    )
    parser.add_argument(
        "--output", "-o", required=True, help="Output path for schema file"
    )
    parser.add_argument("--user-terms", help="JSON file containing user terms")
    parser.add_argument(
        "--additional-databases", nargs="*", help="Additional database names"
    )
    parser.add_argument(
        "--additional-names", nargs="*", help="Additional biological term names"
    )

    args = parser.parse_args()

    # Load user terms if provided
    user_terms = None
    if args.user_terms:
        with open(args.user_terms, "r") as f:
            user_terms = json.load(f)

    # Generate schema
    schema = generate_biological_response_schema(
        user_terms=user_terms,
        additional_databases=args.additional_databases,
        additional_name_terms=args.additional_names,
    )

    # Save schema
    save_schema(schema, args.output)

    print("Schema generated with:")
    print(
        f"  - {len(schema['properties']['coordinate_list']['items']['properties']['name']['enum'])} name options"
    )
    print(
        f"  - {len(schema['properties']['coordinate_list']['items']['properties']['database']['enum'])} database options"
    )
