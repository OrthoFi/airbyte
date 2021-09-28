#
# Copyright (c) 2021 Airbyte, Inc., all rights reserved.
#

import json

import pytest
from airbyte_cdk.sources.utils.transform import TransformConfig, TypeTransformer

SIMPLE_SCHEMA = {"type": "object", "properties": {"value": {"type": "string"}}}
COMPLEX_SCHEMA = {
    "type": "object",
    "properties": {
        "value": {"type": "boolean", "format": "even", "is_positive": True},
        "prop": {"type": "string"},
        "prop_with_null": {"type": ["string", "null"]},
        "number_prop": {"type": "number"},
        "int_prop": {"type": ["integer", "null"]},
        "too_many_types": {"type": ["boolean", "null", "string"]},
        "def": {
            "type": "object",
            "properties": {"dd": {"$ref": "#/definitions/my_type"}},
        },
        "array": {"type": "array", "items": {"$ref": "#/definitions/str_type"}},
        "nested": {"$ref": "#/definitions/nested_type"},
        "list_of_lists": {
            "type": "array",
            "items": {"type": "array", "items": {"type": "string"}},
        },
    },
    "definitions": {
        "str_type": {"type": "string"},
        "nested_type": {"type": "object", "properties": {"a": {"type": "string"}}},
    },
}
VERY_NESTED_SCHEMA = {
    "type": ["null", "object"],
    "properties": {
        "very_nested_value": {
            "type": ["null", "object"],
            "properties": {
                "very_nested_value": {
                    "type": ["null", "object"],
                    "properties": {
                        "very_nested_value": {
                            "type": ["null", "object"],
                            "properties": {
                                "very_nested_value": {
                                    "type": ["null", "object"],
                                    "properties": {"very_nested_value": {"type": ["null", "number"]}},
                                }
                            },
                        }
                    },
                }
            },
        }
    },
}


@pytest.mark.parametrize(
    "schema, actual, expected",
    [
        (
            SIMPLE_SCHEMA,
            {"value": 12},
            {"value": "12"},
        ),
        (
            SIMPLE_SCHEMA,
            {"value": 12},
            {"value": "12"},
        ),
        (
            COMPLEX_SCHEMA,
            {"value": 1, "array": ["111", 111, {1: 111}]},
            {"value": True, "array": ["111", "111", "{1: 111}"]},
        ),
        (
            COMPLEX_SCHEMA,
            {"value": 1, "list_of_lists": [["111"], [111], [11], [{1: 1}]]},
            {"value": True, "list_of_lists": [["111"], ["111"], ["11"], ["{1: 1}"]]},
        ),
        (
            COMPLEX_SCHEMA,
            {"value": 1, "nested": {"a": [1, 2, 3]}},
            {"value": True, "nested": {"a": "[1, 2, 3]"}},
        ),
        (
            COMPLEX_SCHEMA,
            {"value": "false", "nested": {"a": [1, 2, 3]}},
            {"value": False, "nested": {"a": "[1, 2, 3]"}},
        ),
        (COMPLEX_SCHEMA, {}, {}),
        (COMPLEX_SCHEMA, {"int_prop": "12"}, {"int_prop": 12}),
        # Skip invalid formattted field and process other fields.
        (
            COMPLEX_SCHEMA,
            {"prop": 12, "number_prop": "aa12", "array": [12]},
            {"prop": "12", "number_prop": "aa12", "array": ["12"]},
        ),
        # Field too_many_types have ambigious type, skip formatting
        (
            COMPLEX_SCHEMA,
            {"prop": 12, "too_many_types": 1212, "array": [12]},
            {"prop": "12", "too_many_types": 1212, "array": ["12"]},
        ),
        # Test null field
        (
            COMPLEX_SCHEMA,
            {"prop": None, "array": [12]},
            {"prop": "None", "array": ["12"]},
        ),
        # If field can be null do not convert
        (
            COMPLEX_SCHEMA,
            {"prop_with_null": None, "array": [12]},
            {"prop_with_null": None, "array": ["12"]},
        ),
        (
            VERY_NESTED_SCHEMA,
            {"very_nested_value": {"very_nested_value": {"very_nested_value": {"very_nested_value": {"very_nested_value": "2"}}}}},
            {"very_nested_value": {"very_nested_value": {"very_nested_value": {"very_nested_value": {"very_nested_value": 2.0}}}}},
        ),
        (
            VERY_NESTED_SCHEMA,
            {"very_nested_value": {"very_nested_value": None}},
            {"very_nested_value": {"very_nested_value": None}},
        ),
        # Object without properties
        (
            {"type": "object"},
            {"value": 12},
            {"value": 12},
        ),
        (
            # Array without items
            {"type": "object", "properties": {"value": {"type": "array"}}},
            {"value": [12]},
            {"value": [12]},
        ),
        (
            # Array without items and value is not an array
            {"type": "object", "properties": {"value": {"type": "array"}}},
            {"value": "12"},
            {"value": "12"},
        ),
        (
            # Schema root object is not an object, no convertion should happen
            {"type": "integer"},
            {"value": "12"},
            {"value": "12"},
        ),
        (
            # More than one type except null, no conversion should happen
            {"type": "object", "properties": {"value": {"type": ["string", "boolean", "null"]}}},
            {"value": 12},
            {"value": 12},
        ),
        (
            # Oneof not suported, no conversion for one_of_value should happen
            {"type": "object", "properties": {"one_of_value": {"oneOf": ["string", "boolean", "null"]}, "value_2": {"type": "string"}}},
            {"one_of_value": 12, "value_2": 12},
            {"one_of_value": 12, "value_2": "12"},
        ),
    ],
)
def test_transform(schema, actual, expected):
    t = TypeTransformer(TransformConfig.DefaultSchemaNormalization)
    t.transform(actual, schema)
    assert json.dumps(actual) == json.dumps(expected)


def test_transform_wrong_config():
    with pytest.raises(Exception, match="NoTransform option cannot be combined with other flags."):
        TypeTransformer(TransformConfig.NoTransform | TransformConfig.DefaultSchemaNormalization)

    with pytest.raises(Exception, match="Please set TransformConfig.CustomSchemaNormalization config before registering custom normalizer"):

        class NotAStream:
            transformer = TypeTransformer(TransformConfig.DefaultSchemaNormalization)

            @transformer.registerCustomTransform
            def transform_cb(instance, schema):
                pass


def test_custom_transform():
    class NotAStream:
        transformer = TypeTransformer(TransformConfig.CustomSchemaNormalization)

        @transformer.registerCustomTransform
        def transform_cb(instance, schema):
            # Check no default conversion applied
            assert instance == 12
            assert schema == SIMPLE_SCHEMA["properties"]["value"]
            return "transformed"

    s = NotAStream()
    obj = {"value": 12}
    s.transformer.transform(obj, SIMPLE_SCHEMA)
    assert obj == {"value": "transformed"}


def test_custom_transform_with_default_normalization():
    class NotAStream:
        transformer = TypeTransformer(TransformConfig.CustomSchemaNormalization | TransformConfig.DefaultSchemaNormalization)

        @transformer.registerCustomTransform
        def transform_cb(instance, schema):
            # Check default conversion applied
            assert instance == "12"
            assert schema == SIMPLE_SCHEMA["properties"]["value"]
            return "transformed"

    s = NotAStream()
    obj = {"value": 12}
    s.transformer.transform(obj, SIMPLE_SCHEMA)
    assert obj == {"value": "transformed"}
