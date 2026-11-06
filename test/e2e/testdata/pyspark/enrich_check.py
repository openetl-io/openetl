"""The e2e suite's PySpark transform (test/e2e), as openetl-spark's
EnrichCheck: each input becomes the output of its name, with spark_epoch, the
epoch."""

from pyspark.sql import functions as F

import openetl_spark


def transform(spark, inputs, ctx):
    return {name: df.withColumn("spark_epoch", F.lit(ctx.epoch)) for name, df in inputs.items()}


if __name__ == "__main__":
    openetl_spark.run(transform)
