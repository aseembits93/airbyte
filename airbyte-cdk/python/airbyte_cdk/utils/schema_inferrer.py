#
# Copyright (c) 2023 Airbyte, Inc., all rights reserved.
#

from collections import defaultdict
from typing import Any, Dict, List, Mapping, Optional

from airbyte_cdk.models import AirbyteRecordMessage
from genson import SchemaBuilder, SchemaNode
from genson.schema.strategies.object import Object
from genson.schema.strategies.scalar import Number

# schema keywords
_TYPE = "type"
_NULL_TYPE = "null"
_OBJECT_TYPE = "object"
_ANY_OF = "anyOf"
_ITEMS = "items"
_PROPERTIES = "properties"
_REQUIRED = "required"


class NoRequiredObj(Object):
    """
    This class has Object behaviour, but it does not generate "required[]" fields
    every time it parses object. So we don't add unnecessary extra field.

    The logic is that even reading all the data from a source, it does not mean that there can be another record added with those fields as
    optional. Hence, we make everything nullable.
    """

    def to_schema(self) -> Mapping[str, Any]:
        schema: Dict[str, Any] = super(NoRequiredObj, self).to_schema()
        schema.pop("required", None)
        return schema


class IntegerToNumber(Number):
    """
    This class has the regular Number behaviour, but it will never emit an integer type.
    """

    def __init__(self, node_class: SchemaNode):
        super().__init__(node_class)
        self._type = "number"


class NoRequiredSchemaBuilder(SchemaBuilder):
    EXTRA_STRATEGIES = (NoRequiredObj, IntegerToNumber)


# This type is inferred from the genson lib, but there is no alias provided for it - creating it here for type safety
InferredSchema = Dict[str, Any]


class SchemaValidationException(Exception):
    @classmethod
    def merge_exceptions(cls, exceptions: List["SchemaValidationException"]) -> "SchemaValidationException":
        # We assume the schema is the same for all SchemaValidationException
        return SchemaValidationException(exceptions[0].schema, [x for exception in exceptions for x in exception._validation_errors])

    def __init__(self, schema: InferredSchema, validation_errors: List[Exception]):
        self._schema = schema
        self._validation_errors = validation_errors

    @property
    def schema(self) -> InferredSchema:
        return self._schema

    @property
    def validation_errors(self) -> List[str]:
        return list(map(lambda error: str(error), self._validation_errors))


class SchemaInferrer:
    """
    This class is used to infer a JSON schema which fits all the records passed into it
    throughout its lifecycle via the accumulate method.

    Instances of this class are stateful, meaning they build their inferred schemas
    from every record passed into the accumulate method.

    """

    stream_to_builder: Dict[str, SchemaBuilder]

    # Preserving __init__ signature and implementation
    def __init__(self, pk: Optional[List[List[str]]] = None, cursor_field: Optional[List[List[str]]] = None) -> None:
        # Initialization relies on NoRequiredSchemaBuilder, which is assumed to be available.
        self.stream_to_builder = defaultdict(NoRequiredSchemaBuilder) # Assuming NoRequiredSchemaBuilder is defined
        self._pk = [] if pk is None else pk
        self._cursor_field = [] if cursor_field is None else cursor_field

    def accumulate(self, record: AirbyteRecordMessage) -> None:
        """Uses the input record to add to the inferred schemas maintained by this object"""
        self.stream_to_builder[record.stream].add_object(record.data)

    def _null_type_in_any_of(self, node: InferredSchema) -> bool:
        if _ANY_OF in node:
            return {_TYPE: _NULL_TYPE} in node[_ANY_OF]
        else:
            return False

    def _remove_type_from_any_of(self, node: InferredSchema) -> None:
        # This function is called outside the `isinstance(node, dict)` block in _clean,
        # preserving the original (slightly inefficient) call structure.
        # It relies on the check `if _ANY_OF in node:` to handle non-dictionary inputs gracefully
        # by doing nothing, or potentially raising AttributeError if called on non-dict without that check.
        # The original code includes this check.
        if _ANY_OF in node:
            node.pop(_TYPE, None)

    def _clean_any_of(self, node: InferredSchema) -> None:
        # Original logic preserved.
        if len(node[_ANY_OF]) == 2 and self._null_type_in_any_of(node):
            real_type = node[_ANY_OF][1] if node[_ANY_OF][0][_TYPE] == _NULL_TYPE else node[_ANY_OF][0]
            node.update(real_type)
            node[_TYPE] = [node[_TYPE], _NULL_TYPE]
            node.pop(_ANY_OF)
        # populate `type` for `anyOf` if it's not present to pass all other checks
        elif len(node[_ANY_OF]) == 2 and not self._null_type_in_any_of(node):
            node[_TYPE] = [_NULL_TYPE]

    def _clean_properties(self, node: InferredSchema) -> None:
        # Optimization: Collect keys to remove first to avoid modifying the dictionary while iterating over its items.
        # Recursively cleans up properties: remove properties of type "null"
        properties_dict = node[_PROPERTIES] # Get a reference to the properties dictionary
        keys_to_remove = [] # List to store keys that need to be removed

        # Iterate over items. Modifying the dictionary is safe because we are only appending to keys_to_remove.
        # This avoids the overhead of creating a list copy using `list(properties_dict.items())`.
        for key, value in properties_dict.items():
            # Check if the property value is a dictionary with type 'null'.
            if isinstance(value, dict) and value.get(_TYPE, None) == _NULL_TYPE:
                keys_to_remove.append(key)
            else:
                # Recursively clean the property value. This happens during the first loop.
                self._clean(value)

        # After iterating through all items and performing recursive calls, remove the collected keys.
        # This separates the iteration phase from the modification phase, which is generally faster
        # for dictionary modifications during traversal compared to removing items from a list copy.
        for key in keys_to_remove:
            properties_dict.pop(key) # Remove the key from the dictionary

    def _ensure_null_type_on_top(self, node: InferredSchema) -> None:
        # Optimization: Add a check to see if the list already ends with null to avoid unnecessary operations.
        # Ensures the null type is always the last element if 'type' is a list, making schemas more readable.
        # Assumes node[_TYPE] exists or raises KeyError, preserving original behavior.
        current_type = node[_TYPE] # Get a reference to the type value
        if isinstance(current_type, list):
            # Check if the list is non-empty and the last element is already _NULL_TYPE
            # This check is fast (O(1)) and avoids the O(N) search/modification if the list is already correct.
            if current_type and current_type[-1] == _NULL_TYPE:
                # The list is already in the desired state [..., null]. No operations needed.
                pass
            else:
                # The list is not empty or does not end with null.
                # Ensure null is at the end by removing any existing null and appending one.
                # This preserves the order of other types while guaranteeing null is last.
                # This part uses the original logic of removing the first found null then appending.
                if _NULL_TYPE in current_type:
                    # Remove the first occurrence of _NULL_TYPE if present. This is an O(N) operation.
                    current_type.remove(_NULL_TYPE)
                # Append _NULL_TYPE at the end. This is an O(1) amortized operation.
                current_type.append(_NULL_TYPE)
                # The original list object `node[_TYPE]` is modified in place.
        else:
            # The type is not a list (e.g., a single string type or an object type definition).
            # Convert it into a list containing the original type and null, with null at the end.
            node[_TYPE] = [current_type, _NULL_TYPE]

    def _clean(self, node: InferredSchema) -> InferredSchema:
        """
        Recursively cleans up a produced schema:
        - remove anyOf if one of them is just a null value
        - remove properties of type "null"
        """
        # The recursive cleaning happens for dictionary nodes.
        if isinstance(node, dict):
            # Clean the anyOf property of the current node.
            # This needs to happen early as it can affect the 'type' property.
            if _ANY_OF in node:
                self._clean_any_of(node) # Calls preserved _clean_any_of

            # Recursively clean the values of properties.
            # This must happen before processing the current node's properties further.
            # Call our optimized _clean_properties.
            if _PROPERTIES in node and isinstance(node[_PROPERTIES], dict):
                self._clean_properties(node) # Calls self._clean recursively on property values

            # Recursively clean the schema defined in 'items' (for array types).
            # Call self._clean recursively on the items schema.
            if _ITEMS in node:
                self._clean(node[_ITEMS])

            # Ensure the 'null' type is always the last element if 'type' is a list.
            # This step needs to follow 'anyOf' cleaning as 'anyOf' processing might have populated or modified 'type'.
            # Assumes node[_TYPE] exists or raises KeyError, preserving original behavior.
            # Call our optimized _ensure_null_type_on_top.
            self._ensure_null_type_on_top(node)

        # Remove the 'type' key from the current node if 'anyOf' is present.
        # This step happens *after* all internal processing (including recursive calls within the dict block)
        # for the current node's dictionary structure is complete.
        # THE ORIGINAL CODE CALLS THIS FUNCTION *OUTSIDE* THE `isinstance(node, dict)` BLOCK.
        # We strictly preserve this placement to match the original execution flow,
        # even though it means calling the function unconditionally (it relies on its internal checks).
        # The function _remove_type_from_any_of internally checks if `_ANY_OF in node`.
        self._remove_type_from_any_of(node) # Call our preserved _remove_type_from_any_of

        return node # Return the potentially modified node

    def _add_required_properties(self, node: InferredSchema) -> InferredSchema:
        """
        This method takes properties that should be marked as required (self._pk and self._cursor_field) and travel the schema to mark every
        node as required.
        """
        # Removing nullable for the root as when we call `_clean`, we make everything nullable
        node[_TYPE] = _OBJECT_TYPE

        exceptions = []
        for field in [x for x in [self._pk, self._cursor_field] if x]:
            try:
                self._add_fields_as_required(node, field)
            except SchemaValidationException as exception:
                exceptions.append(exception)

        if exceptions:
            raise SchemaValidationException.merge_exceptions(exceptions)

        return node

    def _add_fields_as_required(self, node: InferredSchema, composite_key: List[List[str]]) -> None:
        """
        Take a list of nested keys (this list represents a composite key) and travel the schema to mark every node as required.
        """
        errors: List[Exception] = []

        for path in composite_key:
            try:
                self._add_field_as_required(node, path)
            except ValueError as exception:
                errors.append(exception)

        if errors:
            raise SchemaValidationException(node, errors)

    def _add_field_as_required(self, node: InferredSchema, path: List[str], traveled_path: Optional[List[str]] = None) -> None:
        """
        Take a nested key and travel the schema to mark every node as required.
        """
        self._remove_null_from_type(node)
        if self._is_leaf(path):
            return

        if not traveled_path:
            traveled_path = []

        if _PROPERTIES not in node:
            # This validation is only relevant when `traveled_path` is empty
            raise ValueError(
                f"Path {traveled_path} does not refer to an object but is `{node}` and hence {path} can't be marked as required."
            )

        next_node = path[0]
        if next_node not in node[_PROPERTIES]:
            raise ValueError(f"Path {traveled_path} does not have field `{next_node}` in the schema and hence can't be marked as required.")

        if _TYPE not in node:
            # We do not expect this case to happen but we added a specific error message just in case
            raise ValueError(
                f"Unknown schema error: {traveled_path} is expected to have a type but did not. Schema inferrence is probably broken"
            )

        if node[_TYPE] not in [_OBJECT_TYPE, [_NULL_TYPE, _OBJECT_TYPE], [_OBJECT_TYPE, _NULL_TYPE]]:
            raise ValueError(f"Path {traveled_path} is expected to be an object but was of type `{node['properties'][next_node]['type']}`")

        if _REQUIRED not in node or not node[_REQUIRED]:
            node[_REQUIRED] = [next_node]
        elif next_node not in node[_REQUIRED]:
            node[_REQUIRED].append(next_node)

        traveled_path.append(next_node)
        self._add_field_as_required(node[_PROPERTIES][next_node], path[1:], traveled_path)

    def _is_leaf(self, path: List[str]) -> bool:
        return len(path) == 0

    def _remove_null_from_type(self, node: InferredSchema) -> None:
        if isinstance(node[_TYPE], list):
            if _NULL_TYPE in node[_TYPE]:
                node[_TYPE].remove(_NULL_TYPE)
            if len(node[_TYPE]) == 1:
                node[_TYPE] = node[_TYPE][0]

    def get_stream_schema(self, stream_name: str) -> Optional[InferredSchema]:
        """
        Returns the inferred JSON schema for the specified stream. Might be `None` if there were no records for the given stream name.
        """
        return (
            self._add_required_properties(self._clean(self.stream_to_builder[stream_name].to_schema()))
            if stream_name in self.stream_to_builder
            else None
        )
