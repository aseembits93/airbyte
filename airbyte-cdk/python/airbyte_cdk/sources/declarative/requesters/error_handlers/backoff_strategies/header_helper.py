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
    # Direct dictionary access instead of .get() for slight performance improvement
    # when the header is present (most common case)
    try:
        header_value = response.headers[header]
    except KeyError:
        return None

    # Type check and conversion optimized for the common case (string headers)
    if isinstance(header_value, str):
        if regex:
            match = regex.match(header_value)
            if match:
                header_value = match.group()
        
        # Inline _as_float for better performance (avoid function call overhead)
        try:
            return float(header_value)
        except ValueError:
            return None
    elif isinstance(header_value, numbers.Number):
        return float(header_value)  # type: ignore[arg-type]
    
    return None


def _as_float(s: str) -> Optional[float]:
    try:
        return float(s)
    except ValueError:
        return None
