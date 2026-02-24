from pyspark.sql import Row
from pyspark.sql.types import StructType, StructField, StringType, DoubleType
from breweries_core import standardize, add_quality_flags, add_hash, build_silver_df

RAW_SCHEMA = StructType([
    StructField("id", StringType(), True),
    StructField("name", StringType(), True),
    StructField("brewery_type", StringType(), True),
    StructField("address_1", StringType(), True),
    StructField("address_2", StringType(), True),
    StructField("address_3", StringType(), True),
    StructField("street", StringType(), True),
    StructField("postal_code", StringType(), True),
    StructField("city", StringType(), True),
    StructField("state", StringType(), True),
    StructField("state_province", StringType(), True),
    StructField("country", StringType(), True),
    StructField("latitude", StringType(), True),   
    StructField("longitude", StringType(), True), 
    StructField("phone", StringType(), True),
    StructField("website_url", StringType(), True),
])


def test_silver_standardize_and_flags(spark):
    df_raw = spark.createDataFrame([
        Row(
            id="1",
            name="  Brewery A  ",
            brewery_type="micro",
            address_1="Street 1",
            address_2=None,
            address_3=None,
            street="Street 1",
            postal_code="00000",
            city="  sao paulo ",
            state="sp",
            state_province=None,
            country="brazil",
            latitude="10.123456789",
            longitude="20.987654321",
            phone="(11) 9999-9999",
            website_url="https://a.com",
        ),
        Row(
            id="2",
            name="Bad\uFFFDName",
            brewery_type="micro",
            address_1=None,
            address_2=None,
            address_3=None,
            street=None,
            postal_code=None,
            city=None,
            state=None,
            state_province=None,
            country=None,
            latitude=None,
            longitude=None,
            phone=None,
            website_url=None,
        ),
    ], schema=RAW_SCHEMA)

    df = build_silver_df(add_hash(add_quality_flags(standardize(df_raw))))
    rows = {r["id"]: r for r in df.collect()}

    assert rows["1"]["name"] == "Brewery A"
    assert rows["1"]["city"] == "Sao Paulo"
    assert rows["1"]["country"] == "Brazil"
    assert rows["1"]["has_website"] is True
    assert rows["1"]["has_address"] is True
    assert rows["1"]["has_geolocation"] is True
    assert rows["1"]["has_proper_encoding"] is True
    assert abs(rows["1"]["latitude"] - 10.123457) < 1e-9
    assert abs(rows["1"]["longitude"] - 20.987654) < 1e-9

    assert rows["2"]["has_proper_encoding"] is False
    assert rows["2"]["has_website"] is False
    assert rows["2"]["has_address"] is False
    assert rows["2"]["has_geolocation"] is False

def test_silver_hash_changes_on_business_change(spark):
    df1 = spark.createDataFrame([
        Row(
            id="1", name="A", brewery_type="micro",
            address_1="X", address_2=None, address_3=None, street=None,
            postal_code="1", city="C", state="S", state_province=None, country="U",
            latitude=1.0, longitude=2.0, phone="123", website_url=None
        )
    ])
    h1 = build_silver_df(add_hash(add_quality_flags(standardize(df1)))).select("hash_merge").collect()[0][0]

    df2 = spark.createDataFrame([
        Row(
            id="1", name="A", brewery_type="micro",
            address_1="Y", address_2=None, address_3=None, street=None,  # mudou
            postal_code="1", city="C", state="S", state_province=None, country="U",
            latitude=1.0, longitude=2.0, phone="123", website_url=None
        )
    ])
    h2 = build_silver_df(add_hash(add_quality_flags(standardize(df2)))).select("hash_merge").collect()[0][0]

    assert h1 != h2