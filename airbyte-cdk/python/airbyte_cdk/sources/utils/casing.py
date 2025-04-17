#
# Copyright (c) 2023 Airbyte, Inc., all rights reserved.
#


import re


# https://stackoverflow.com/a/1176023
def camel_to_snake(s: str) -> str:
    # Combining the two regex operations into one to minimize regex calls
    s = re.sub(r'(?<=[a-z0-9])(?=[A-Z])|(?<=.)(?=[A-Z][a-z])', '_', s).lower()
    return s
