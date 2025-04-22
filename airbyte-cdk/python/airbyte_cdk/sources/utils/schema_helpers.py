#
# Copyright (c) 2023 Airbyte, Inc., all rights reserved.
#


import importlib
import json
import os
import pkgutil
from typing import Any, ClassVar, Dict, List, Mapping, MutableMapping, Optional, Tuple

import jsonref
from airbyte_cdk.models import ConnectorSpecification, FailureType
from airbyte_cdk.utils.traced_exception import AirbyteTracedException
from jsonschema import RefResolver, validate
from jsonschema.exceptions import ValidationError
from pydantic.v1 import BaseModel, Field


class JsonFileLoader:
    """
    Custom json file loader to resolve references to resources located in "shared" directory.
    We need this for compatability with existing schemas cause all of them have references
    pointing to shared_schema.json file instead of shared/shared_schema.json
    """

    def __init__(self, uri_base: str, shared: str):
        self.shared = shared
        self.uri_base = uri_base

    def __call__(self, uri: str) -> Dict[str, Any]:
        uri = uri.replace(self.uri_base, f"{self.uri_base}/{self.shared}/")
        with open(uri) as f:
            data = json.load(f)
            if isinstance(data, dict):
                return data
            else:
                raise ValueError(f"Expected to read a dictionary from {uri}. Got: {data}")


def resolve_ref_links(obj: Any) -> Any:
    """
    Scan resolved schema and convert jsonref.JsonRef object to JSON serializable dict.

    :param obj - jsonschema object with ref field resolved.
    :return JSON serializable object with references without external dependencies.
    """
    # Use a dictionary within the scope of this call to cache processed objects by their ID.
    # This ensures the cache is clean for each top-level invocation of resolve_ref_links
    # and handles potential cycles in the object graph by returning incomplete objects
    # during processing, which will be filled later.
    processed_cache = {}

    def _resolve_recursive(current_obj: Any) -> Any:
        # Only attempt to cache mutable containers (dicts, lists) and JsonRef instances,
        # as they are potentially large, can be duplicated by reference, and are suitable for ID-based caching.
        # Primitives and other immutable types are cheap to process and not cached by ID.
        is_cacheable = isinstance(current_obj, (dict, list, jsonref.JsonRef))

        if is_cacheable:
             obj_id = id(current_obj)
             if obj_id in processed_cache:
                 # Return the already processed object if found in cache.
                 # For cyclic references, this might return an object that is still
                 # being populated, which is the desired behavior for cycles.
                 return processed_cache[obj_id]

        # --- Core Transformation Logic ---

        if isinstance(current_obj, jsonref.JsonRef):
            # Recursively resolve the subject of the JsonRef.
            # The recursive call handles the cache check for the subject object itself.
            resolved_subject = _resolve_recursive(current_obj.__subject__)

            # Apply JsonRef-specific cleanup (remove definitions) if the subject resolved to a dict.
            if isinstance(resolved_subject, dict):
                # Create a copy to ensure we don't modify a potentially cached or shared object instance
                # that the subject might be pointing to. Each JsonRef should yield a unique cleaned dictionary.
                result_obj = resolved_subject.copy()
                # Omit existing definitions for external resource since
                # we dont need it anymore.
                result_obj.pop("definitions", None)
            else:
                # The original code expects the JsonRef subject to resolve to a dict.
                # Preserve the original ValueError message template, using the actual resolved value.
                raise ValueError(f"Expected obj to be a dict. Got {resolved_subject}")

            # Cache the final result for this specific JsonRef object after the transformation.
            # This ensures that if the same JsonRef instance is encountered again (less common
            # after jsonref resolution, but possible), its processed result is reused.
            if is_cacheable: # This is true for JsonRef
                 processed_cache[obj_id] = result_obj

            return result_obj

        elif isinstance(current_obj, dict):
            # Create a new dictionary to build the processed result into.
            processed_dict = {}
            # Cache the new dictionary *before* processing its children.
            # This is crucial for handling recursive/cyclic references within the structure:
            # if a child object contains a reference back to this dictionary, the recursive call
            # for that child will find this incomplete dictionary in the cache and use it,
            # preventing infinite recursion and correctly building the cyclic structure.
            if is_cacheable: # This is true for dict
                 processed_cache[obj_id] = processed_dict

            # Process dictionary values recursively.
            for k, v in current_obj.items():
                processed_dict[k] = _resolve_recursive(v)

            # The processed_dict is now fully populated. It was already placed in the cache.
            return processed_dict

        elif isinstance(current_obj, list):
            # Create a new list to build the processed result into.
            processed_list = []
            # Cache the new list *before* processing its children.
            # This is crucial for handling recursive/cyclic references.
            if is_cacheable: # This is true for list
                 processed_cache[obj_id] = processed_list

            # Process list items recursively.
            for item in current_obj:
                processed_list.append(_resolve_recursive(item))

            # The processed_list is now fully populated. It was already placed in the cache.
            return processed_list

        else:
            # For primitives and other non-container types, return the object directly.
            # They are not cached by ID as they are immutable or cheaply copied implicitly.
            return current_obj

    # Start the recursive traversal and transformation process with the initial object.
    # The cache is managed internally by the helper function _resolve_recursive.
    return _resolve_recursive(obj)


def _expand_refs(schema: Any, ref_resolver: Optional[RefResolver] = None) -> None:
    """Internal function to iterate over schema and replace all occurrences of $ref with their definitions. Recursive.

    :param schema: schema that will be patched
    :param ref_resolver: resolver to get definition from $ref, if None pass it will be instantiated
    """
    ref_resolver = ref_resolver or RefResolver.from_schema(schema)

    if isinstance(schema, MutableMapping):
        if "$ref" in schema:
            ref_url = schema.pop("$ref")
            _, definition = ref_resolver.resolve(ref_url)
            _expand_refs(definition, ref_resolver=ref_resolver)  # expand refs in definitions as well
            schema.update(definition)
        else:
            for key, value in schema.items():
                _expand_refs(value, ref_resolver=ref_resolver)
    elif isinstance(schema, List):
        for value in schema:
            _expand_refs(value, ref_resolver=ref_resolver)


def expand_refs(schema: Any) -> None:
    """Iterate over schema and replace all occurrences of $ref with their definitions.

    :param schema: schema that will be patched
    """
    _expand_refs(schema)
    schema.pop("definitions", None)  # remove definitions created by $ref


def rename_key(schema: Any, old_key: str, new_key: str) -> None:
    """Iterate over nested dictionary and replace one key with another. Used to replace anyOf with oneOf. Recursive."

    :param schema: schema that will be patched
    :param old_key: name of the key to replace
    :param new_key: new name of the key
    """
    if not isinstance(schema, MutableMapping):
        return

    for key, value in schema.items():
        rename_key(value, old_key, new_key)
        if old_key in schema:
            schema[new_key] = schema.pop(old_key)


class ResourceSchemaLoader:
    """JSONSchema loader from package resources"""

    def __init__(self, package_name: str):
        self.package_name = package_name

    def get_schema(self, name: str) -> dict[str, Any]:
        """
        This method retrieves a JSON schema from the schemas/ folder.


        The expected file structure is to have all top-level schemas (corresponding to streams) in the "schemas/" folder, with any shared $refs
        living inside the "schemas/shared/" folder. For example:

        schemas/shared/<shared_definition>.json
        schemas/<name>.json # contains a $ref to shared_definition
        schemas/<name2>.json # contains a $ref to shared_definition
        """

        schema_filename = f"schemas/{name}.json"
        raw_file = pkgutil.get_data(self.package_name, schema_filename)
        if not raw_file:
            raise IOError(f"Cannot find file {schema_filename}")
        try:
            raw_schema = json.loads(raw_file)
        except ValueError as err:
            raise RuntimeError(f"Invalid JSON file format for file {schema_filename}") from err

        return self._resolve_schema_references(raw_schema)

    def _resolve_schema_references(self, raw_schema: dict[str, Any]) -> dict[str, Any]:
        """
        Resolve links to external references and move it to local "definitions" map.

        :param raw_schema jsonschema to lookup for external links.
        :return JSON serializable object with references without external dependencies.
        """

        package = importlib.import_module(self.package_name)
        if package.__file__:
            base = os.path.dirname(package.__file__) + "/"
        else:
            raise ValueError(f"Package {package} does not have a valid __file__ field")
        resolved = jsonref.JsonRef.replace_refs(raw_schema, loader=JsonFileLoader(base, "schemas/shared"), base_uri=base)
        resolved = resolve_ref_links(resolved)
        if isinstance(resolved, dict):
            return resolved
        else:
            raise ValueError(f"Expected resolved to be a dict. Got {resolved}")


def check_config_against_spec_or_exit(config: Mapping[str, Any], spec: ConnectorSpecification) -> None:
    """
    Check config object against spec. In case of spec is invalid, throws
    an exception with validation error description.

    :param config - config loaded from file specified over command line
    :param spec - spec object generated by connector
    """
    spec_schema = spec.connectionSpecification
    try:
        validate(instance=config, schema=spec_schema)
    except ValidationError as validation_error:
        raise AirbyteTracedException(
            message="Config validation error: " + validation_error.message,
            internal_message=validation_error.message,
            failure_type=FailureType.config_error,
        ) from None  # required to prevent logging config secrets from the ValidationError's stacktrace


class InternalConfig(BaseModel):
    KEYWORDS: ClassVar[set[str]] = {"_limit", "_page_size"}
    limit: int = Field(None, alias="_limit")
    page_size: int = Field(None, alias="_page_size")

    def dict(self, *args: Any, **kwargs: Any) -> dict[str, Any]:
        kwargs["by_alias"] = True
        kwargs["exclude_unset"] = True
        return super().dict(*args, **kwargs)  # type: ignore[no-any-return]

    def is_limit_reached(self, records_counter: int) -> bool:
        """
        Check if record count reached limit set by internal config.
        :param records_counter - number of records already red
        :return True if limit reached, False otherwise
        """
        if self.limit:
            if records_counter >= self.limit:
                return True
        return False


def split_config(config: Mapping[str, Any]) -> Tuple[dict[str, Any], InternalConfig]:
    """
    Break config map object into 2 instances: first is a dict with user defined
    configuration and second is internal config that contains private keys for
    acceptance test configuration.

    :param
     config - Dict object that has been loaded from config file.

    :return tuple of user defined config dict with filtered out internal
    parameters and connector acceptance test internal config object.
    """
    main_config = {}
    internal_config = {}
    for k, v in config.items():
        if k in InternalConfig.KEYWORDS:
            internal_config[k] = v
        else:
            main_config[k] = v
    return main_config, InternalConfig.parse_obj(internal_config)
