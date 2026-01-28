import logging

import requests

# from typing import Dict, List, Optional

# from polars_dovmed.utils import setup_logging


# Initialize logger
logger = logging.getLogger(__name__)


def list_available_models(api_base: str, api_key: str) -> list:
    """
    List available models from the API.

    Args:
        api_base: API base URL
        api_key: API key for authentication

    Returns:
        List of available model names
    """
    headers = {"Authorization": f"Bearer {api_key}", "Content-Type": "application/json"}

    try:
        response = requests.get(
            f"{api_base.rstrip('/')}/v1/models", headers=headers, timeout=30
        )
        response.raise_for_status()

        result = response.json()
        models = [model["id"] for model in result.get("data", [])]

        logger.debug(f"Available models: {models}")
        return models

    except Exception as e:
        logger.warning(f"Could not fetch available models: {e}")
        return []


def normalize_model_name(model: str, available_models: list = []) -> str:
    """
    Normalize model name by removing provider prefix if needed.

    Args:
        model: Original model name
        available_models: List of available models (optional)

    Returns:
        Normalized model name
    """
    original_model = model

    # If model name contains provider prefix, try removing it
    if "/" in model:
        # Try without the provider prefix
        model_without_prefix = model.split("/", 1)[1]

        # If we have available models, check if the version without prefix exists
        if available_models:
            if model_without_prefix in available_models:
                logger.debug(
                    f"Using model name without prefix: {model_without_prefix} (was {original_model})"
                )
                return model_without_prefix
            elif model in available_models:
                logger.debug(f"Using original model name: {model}")
                return model
            else:
                logger.warning(
                    f"Model {model} not found in available models. Trying without prefix: {model_without_prefix}"
                )
                return model_without_prefix
        else:
            logger.debug(
                f"Trying model name without prefix: {model_without_prefix} (was {original_model})"
            )
            return model_without_prefix

    return model


def call_llm_api(
    system_prompt: str,
    user_prompt: str,
    model: str,
    api_base: str,
    api_key: str,
    max_tokens: int = 1000,
    temperature: float = 0.01,
) -> str:
    """
    Call the LLM API to generate query patterns.

    Args:
        input_text: The topic to generate patterns for
        model: Model name to use
        api_base: API base URL
        api_key: API key for authentication
        max_tokens: Maximum tokens in response
        temperature: Temperature for generation (lower = more deterministic)

    Returns:
        Raw response text from the LLM
    """
    payload = {
        "model": model,
        "messages": [
            {"role": "system", "content": system_prompt},
            {"role": "user", "content": user_prompt},
        ],
        "max_tokens": max_tokens,
        "temperature": temperature,
    }

    headers = {"Authorization": f"Bearer {api_key}", "Content-Type": "application/json"}

    # logger.debug(f"Calling LLM API: {api_base}")
    # logger.debug(f"Model: {model}")
    # logger.debug(f"system prompt: {system_prompt}")
    # logger.debug(f"user prompt: {user_prompt}")

    try:
        response = requests.post(
            f"{api_base.rstrip('/')}/chat/completions",
            json=payload,
            headers=headers,
            timeout=90,
        )

        # Log the response details for debugging
        # logger.debug(f"Response status code: {response.status_code}")
        # logger.debug(f"Response headers: {dict(response.headers)}")

        if not response.ok:
            logger.error(f"API error response body: {response.text}")

        response.raise_for_status()

        result = response.json()
        content = result["choices"][0]["message"]["content"].strip()

        # # logger.debug("LLM API call successful")
        # logger.debug(f"Raw response: {content}")

        return content

    except requests.exceptions.RequestException as e:
        logger.error(f"API request failed: {e}")
        raise
    except KeyError as e:
        logger.error(f"Unexpected API response format: {e}")
        logger.error(f"Full response: {result}")
        raise
    except Exception as e:
        logger.error(f"Unexpected error during API call: {e}")
        raise
