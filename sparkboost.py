import pyspark
import pandas as pd
from typing import List, Union
from pyspark.sql.functions import col, sum
from pyspark.sql import DataFrame, functions as F, types as T


def _isnull(df: DataFrame) -> pd.DataFrame:
    return df.agg(
        *[sum(col(c).isNull().cast("int")).alias(c) for c in df.columns]
    ).toPandas()


def isnull(df: DataFrame, normalize: bool = False) -> pd.Series:
    x = _isnull(df)
    y = list(x.values[0])
    c = df.columns
    d = {k: v for k, v in zip(c, y)}
    if normalize:
        return (pd.Series(d) / df.count()).map("{:.2%}".format)
    else:
        return pd.Series(d)


# def ctype(df: DataFrame, columns: List, dtype: T):
#     for c in columns:
#         df = df.withColumn(c, F.col(c).cast(dtype()))


# def ctype(df: DataFrame, columns: Union[str, List[str]], dtype: T):
#     if isinstance(columns, str):
#         columns = [columns]
#     for c in columns:
#         df = df.withColumn(c, F.col(c).cast(dtype()))
#         return df


def head(df: DataFrame, n: int = 5) -> pd.DataFrame:
    # return df.limit(n).toPandas()
    return pd.DataFrame(df.take(n), columns=df.columns)


# Define the custom DataFrame class
class SBFrame(DataFrame):
    def __init__(self, df, spark):
        super().__init__(df._jdf, spark)

    # Add the additional method to the custom class
    head = head
    isnull = isnull
    # ctype = ctype


# # Define the custom DataFrame class
# class SBFrame(DataFrame):
#     def __init__(self, df):
#         super().__init__(df._jdf, df.sql_ctx)
