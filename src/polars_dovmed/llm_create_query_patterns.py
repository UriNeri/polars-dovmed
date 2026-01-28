"""
LLM-based query pattern generation for literature mining.
"""

import argparse
import json
import logging
import sys
from pathlib import Path
from typing import Dict  # , List, Optional

from dotenv import load_dotenv

from polars_dovmed.llm_utils import (
    call_llm_api,
    list_available_models,
    normalize_model_name,
)
from polars_dovmed.schema_utils import generate_biological_response_schema, save_schema
from polars_dovmed.utils import setup_logging

# Load environment variables
load_dotenv()

# Initialize logger
logger = logging.getLogger(__name__)


def create_system_prompt(prompt_file: str | None | Path = None) -> str:
    """
    Create the system prompt that explains to the LLM what response format is expected.
    """
    if not prompt_file:
        prompt_file = (
            Path(__file__).parent.parent
            / "assets"
            / "prompts"
            / "pattern_groups_query.txt"
        )
    with open(prompt_file, "r") as f:
        prompt = f.read()
    return prompt


def create_user_prompt(input_text: str) -> str:
    """
    Create the user prompt with the specific input text.
    """
    return f"""
User query: {input_text}

"""


def save_patterns(patterns: Dict[str, str], output_file: str) -> None:
    """
    Save the generated patterns to a JSON file.

    Args:
        patterns: Dictionary of query patterns
        output_file: Path to output file
    """
    output_path = Path(output_file)
    output_path.parent.mkdir(parents=True, exist_ok=True)

    with open(output_path, "w", encoding="utf-8") as f:
        json.dump(patterns, f, indent=2, ensure_ascii=False)

    logger.info(f"Saved {len(patterns)} patterns to {output_file}")


def parse_llm_response(response_text: str) -> Dict[str, str]:
    """
    Parse the LLM response and extract the JSON query patterns.

    Args:
        response_text: Raw response from LLM

    Returns:
        Dictionary of query patterns
    """
    # Try to find JSON in the response
    response_text = response_text.strip()

    # Remove markdown code blocks if present
    if response_text.startswith("```json"):
        response_text = response_text[7:]
    if response_text.startswith("```"):
        response_text = response_text[3:]
    if response_text.endswith("```"):
        response_text = response_text[:-3]

    response_text = response_text.strip()

    try:
        patterns = json.loads(response_text)

        if not isinstance(patterns, dict):
            raise ValueError("Response is not a JSON object")

        # Convert patterns to the expected format
        logger.info(f"Successfully parsed {len(patterns)} query patterns")
        for key, pattern in patterns.items():
            logger.info(f"  {key}: {pattern}")

        return patterns

    except json.JSONDecodeError as e:
        logger.error(f"Failed to parse JSON response: {e}")
        logger.error(f"Response text: {response_text}")
        raise ValueError(f"Invalid JSON response from LLM: {e}")
    except Exception as e:
        logger.error(f"Error parsing LLM response: {e}")
        raise


def main():
    """
    Main function to generate query patterns using LLM.
    """
    parser = argparse.ArgumentParser(
        description="Generate query patterns for literature mining using LLM",
        formatter_class=argparse.RawDescriptionHelpFormatter,
        epilog="""
Example usage:
  python src/polars_dovmed/llm_create_query_patterns.py \
    --input-text "RNA secondary structure elements and motifs used in the genomes of RNA viruses" \
    --output-file "data/assets/patterns/rna_virus_queries.json" \
    --model "Llama-4-Scout-17B-16E-Instruct" \
    --api-base "https://api.openai.com/v1" \
    --api-key $LLM_API_KEY \
    --n-patterns 10 \
    --schema-output "data/assets/patterns/rna_virus_schema.json" \
    --additional-databases "Rfam" "VirDB" 
""",
    )

    parser.add_argument(
        "--input-text",
        required=True,
        help="Description of the topic to generate search patterns for",
    )

    parser.add_argument(
        "--output-file",
        required=True,
        help="Path to save the generated query patterns JSON file",
    )

    parser.add_argument(
        "--model",
        required=True,
        help="LLM model name to use (e.g., 'Devstral-Small-2505')",
    )

    parser.add_argument(
        "--api-base",
        required=True,
        help="Base URL for the LLM API (e.g., 'https://api.openai.com/v1')",
    )

    parser.add_argument("--api-key", required=True, help="API key for authentication")

    parser.add_argument(
        "--max-tokens",
        type=int,
        default=10000,
        help="Maximum tokens in LLM response (default: 1000)",
    )

    parser.add_argument(
        "--temperature",
        type=float,
        default=0.1,
        help="Temperature for LLM generation (default: 0.1)",
    )
    parser.add_argument(
        "--n-patterns",
        type=int,
        default=10,
        help="total number of patterns to have the llm generate",
    )
    parser.add_argument(
        "--schema-output",
        required=False,
        help="(Optional) path to save JSON schema on how output of analysis using the generated patterns should be look like",
    )
    parser.add_argument(
        "--additional-databases",
        nargs="*",
        help="Additional database names to include in schema",
    )
    parser.add_argument(
        "--verbose", "-v", action="store_true", help="Enable verbose logging"
    )
    parser.add_argument(
        "--log-file",
        default="logs/llm_create_query_patterns.log",
        help="path to save the log",
    )
    parser.add_argument(
        "--prompt-file",
        required=False,
        default=None,
        help="path to a custom system prompt file",
    )
    args = parser.parse_args()

    # Setup logging
    setup_logging(log_file=args.log_file, verbose=args.verbose)

    try:
        logger.info("Starting LLM query pattern generation")
        logger.info(f"Input text: {args.input_text}")
        logger.info(f"Output file: {args.output_file}")
        logger.info(f"Model: {args.model}")
        logger.info(f"API base: {args.api_base}")

        # Get available models and normalize model name
        available_models = list_available_models(args.api_base, args.api_key)
        normalized_model = normalize_model_name(args.model, available_models)

        if normalized_model != args.model:
            logger.info(f"Using normalized model name: {normalized_model}")

        # logger.info(f"Input text: {args.input_text}")
        system_prompt = create_system_prompt(prompt_file=args.prompt_file)
        user_prompt = create_user_prompt(args.input_text)

        # Call LLM to generate patterns
        response_text = call_llm_api(
            user_prompt=user_prompt,
            system_prompt=system_prompt,
            model=normalized_model,
            api_base=args.api_base,
            api_key=args.api_key,
            max_tokens=args.max_tokens,
            temperature=args.temperature,
        )

        # Parse the response
        patterns = parse_llm_response(response_text)

        # Save the patterns
        save_patterns(patterns, args.output_file)

        # Generate and save schema if requested
        if args.schema_output:
            logger.info("Generating JSON schema based on query patterns...")
            schema = generate_biological_response_schema(
                user_terms=patterns, additional_databases=args.additional_databases
            )
            save_schema(schema, args.schema_output)
            logger.info(f"Schema saved to: {args.schema_output}")

        logger.info("✅ Successfully generated and saved query patterns")
        logger.info(f"Generated {len(patterns)} query patterns:")
        for name in patterns.keys():
            if name not in ["virus_taxonomy_report", "disqualifying_terms"]:
                logger.info(f"  - {name}")

    except Exception as e:
        logger.error(f"❌ Failed to generate query patterns: {e}")
        sys.exit(1)


if __name__ == "__main__":
    main()
