import pytest
from pyspark.sql import Row
from python_lib.breweries_core import validate_flag_threshold

def test_validate_flag_threshold_raises(spark):
    df = spark.createDataFrame([
        Row(id="1", has_website=True),
        Row(id="2", has_website=False),
        Row(id="3", has_website=False),
        Row(id="4", has_website=False),
    ])
    with pytest.raises(Exception):
        validate_flag_threshold(df, "has_website", 0.60)

def test_validate_flag_threshold_passes(spark):
    df = spark.createDataFrame([
        Row(id="1", has_address=True),
        Row(id="2", has_address=True),
        Row(id="3", has_address=False),
    ])
    validate_flag_threshold(df, "has_address", 0.50)