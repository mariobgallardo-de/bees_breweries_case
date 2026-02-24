from pyspark.sql import Row
from python_lib.breweries_core import build_gold

def test_gold_aggregation_filters_and_counts(spark):
    df_silver = spark.createDataFrame([
        Row(id="1", country="US", state="CA", city="LA", brewery_type="micro", has_proper_encoding=True,  has_address=True),
        Row(id="2", country="US", state="CA", city="LA", brewery_type="micro", has_proper_encoding=True,  has_address=True),
        Row(id="3", country="US", state="CA", city="SF", brewery_type="brewpub", has_proper_encoding=True, has_address=True),
        Row(id="4", country="US", state="CA", city="SF", brewery_type="brewpub", has_proper_encoding=False, has_address=True), # filtrado
        Row(id="5", country="US", state="CA", city="SF", brewery_type="brewpub", has_proper_encoding=True, has_address=False), # filtrado
    ])

    out = build_gold(df_silver).collect()
    got = {(r["country"], r["state"], r["city"], r["brewery_type"], r["brewery_count"]) for r in out}

    assert ("US", "CA", "LA", "micro", 2) in got
    assert ("US", "CA", "SF", "brewpub", 1) in got