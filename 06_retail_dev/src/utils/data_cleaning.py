from pyspark.sql import functions as F
from pyspark.sql.types import StringType
from pyspark.sql import DataFrame

def clean_dataframe(df: DataFrame) -> DataFrame:
    """
    Remove whitespace from column names and trim string values
    """

    # 1. Clean column names
    new_cols = [c.strip().replace(" ", "_") for c in df.columns]
    df = df.toDF(*new_cols)

    # 2. Trim string values
    for field in df.schema.fields:
        if isinstance(field.dataType, StringType):
            df = df.withColumn(field.name, F.trim(F.col(field.name)))
    return df
