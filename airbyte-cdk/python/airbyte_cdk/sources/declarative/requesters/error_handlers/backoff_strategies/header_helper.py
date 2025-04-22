#
# Copyright (c) 2023 Airbyte, Inc., all rights reserved.
#

import numbers
from re import Pattern
from typing import Optional

import requests


def get_numeric_value_from_header(response: requests.Response, header: str, regex: Optional[Pattern[str]]) -> Optional[float]:
    """
    Extract a header value from the response as a float
    :param response: response the extract header value from
    :param header: Header to extract
    :param regex: optional regex to apply on the header to obtain the value
    :return: header value as float if it's a number. None otherwise
    """
    # Use .get() with default None to handle missing headers safely.
    header_value = response.headers.get(header, None)

    # Check for None or empty string first, consistent with original 'if not header_value:'
    # This correctly handles cases where the header is missing (None) or explicitly empty ("").
    # Note: 'if not header_value:' would also treat 0, 0.0, False, empty list/dict as falsey,
    # but response headers are typically strings or None, so this primarily handles None and "".
    if not header_value:
        return None

    processed_value = header_value

    # Apply regex only if regex is provided AND the header value is a string.
    # HTTP header values are typically strings, but checking isinstance is safer.
    if regex and isinstance(header_value, str):
        match = regex.match(header_value)
        # If regex matched, use the matched part for conversion.
        # If regex didn't match, processed_value remains the original header_value,
        # matching the original behavior where the full string would be passed to _as_float.
        if match:
            processed_value = match.group()

    # Attempt to convert the processed value to a float.
    # This single float() call replaces the original branching logic based on isinstance
    # and the call to the helper function _as_float.
    # The float() constructor can convert string representations of numbers, integers,
    # floats, and booleans (True to 1.0, False to 0.0).
    # It raises ValueError for non-numeric strings and TypeError for other incompatible types.
    try:
        return float(processed_value)
    except (ValueError, TypeError):
        # If float() conversion fails (e.g., non-numeric string, or processed_value
        # was an unexpected type after regex check), return None.
        # This matches the failure cases of the original _as_float and the final else block.
        return None


def _as_float(s: str) -> Optional[float]:
    try:
        return float(s)
    except ValueError:
        return None
