# Databricks notebook source
# Helpers for stable schema inference from API records

import json

from pyspark.sql import types as T


def infer_data_type(value):
    if value is None:
        return T.NullType()
    if isinstance(value, bool):
        return T.BooleanType()
    if isinstance(value, int):
        return T.LongType()
    if isinstance(value, float):
        return T.DoubleType()
    if isinstance(value, str):
        return T.StringType()
    if isinstance(value, dict):
        return T.StructType(
            [
                T.StructField(key, infer_data_type(nested_value), True)
                for key, nested_value in sorted(value.items())
            ]
        )
    if isinstance(value, list):
        element_type = T.NullType()
        for item in value:
            element_type = merge_data_types(element_type, infer_data_type(item))
        if isinstance(element_type, T.NullType):
            element_type = T.StringType()
        return T.ArrayType(element_type, True)
    return T.StringType()


def merge_data_types(left_type, right_type):
    if isinstance(left_type, T.NullType):
        return right_type
    if isinstance(right_type, T.NullType):
        return left_type
    if type(left_type) is type(right_type):
        if isinstance(left_type, T.StructType):
            fields = {field.name: field.dataType for field in left_type.fields}
            for field in right_type.fields:
                fields[field.name] = merge_data_types(
                    fields.get(field.name, T.NullType()),
                    field.dataType
                )
            return T.StructType(
                [
                    T.StructField(name, data_type, True)
                    for name, data_type in sorted(fields.items())
                ]
            )
        if isinstance(left_type, T.ArrayType):
            return T.ArrayType(
                merge_data_types(left_type.elementType, right_type.elementType),
                True
            )
        return left_type
    if (
        isinstance(left_type, (T.LongType, T.DoubleType))
        and isinstance(right_type, (T.LongType, T.DoubleType))
    ):
        return T.DoubleType()
    return T.StringType()


def infer_records_schema(records):
    schema = T.StructType([])
    for record in records:
        schema = merge_data_types(schema, infer_data_type(record))
    return schema


def normalize_value(value, data_type):
    if value is None:
        return None
    if isinstance(data_type, T.DoubleType):
        return float(value)
    if isinstance(data_type, T.LongType):
        return int(value)
    if isinstance(data_type, T.BooleanType):
        return bool(value)
    if isinstance(data_type, T.StringType):
        if isinstance(value, (dict, list)):
            return json.dumps(value)
        return str(value)
    if isinstance(data_type, T.StructType):
        if not isinstance(value, dict):
            return None
        return {
            field.name: normalize_value(value.get(field.name), field.dataType)
            for field in data_type.fields
        }
    if isinstance(data_type, T.ArrayType):
        if not isinstance(value, list):
            return None
        return [normalize_value(item, data_type.elementType) for item in value]
    return value


def normalize_record(record, schema):
    return {
        field.name: normalize_value(record.get(field.name), field.dataType)
        for field in schema.fields
    }
