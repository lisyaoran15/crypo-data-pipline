from pyspark.sql.types import (
    StructType, StructField,
    LongType, DoubleType
)

SCHEMA = StructType([
    StructField("open_time",       LongType(),   True),
    StructField("open",            DoubleType(), True),
    StructField("high",            DoubleType(), True),
    StructField("low",             DoubleType(), True),
    StructField("close",           DoubleType(), True),
    StructField("volume",          DoubleType(), True),
    StructField("close_time",      LongType(),   True),
    StructField("quote_volume",    DoubleType(), True),
    StructField("num_trades",      LongType(),   True),
    StructField("taker_buy_base",  DoubleType(), True),
    StructField("taker_buy_quote", DoubleType(), True),
    StructField("ignore",          LongType(),   True),
])


