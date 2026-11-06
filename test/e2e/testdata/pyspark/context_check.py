"""The e2e suite's PySpark post-sync application (test/e2e), as openetl-spark's
ContextCheck: it reads the sync's context, runs a job on the executors, and
writes what it read to the file:// path in its first argument."""

import json
import sys

from pyspark.sql import SparkSession

from openetl_spark import SyncContext

spark = SparkSession.builder.getOrCreate()
try:
    ctx = SyncContext.load(spark)
    rows = sum(s.rows for s in ctx.streams)
    out = {"sync": ctx.sync, "pipeline": ctx.pipeline, "rows": rows, "counted": spark.range(rows).count(), "through": ctx.through.epoch}
    with open(sys.argv[1].removeprefix("file://"), "w") as f:
        json.dump(out, f)
finally:
    spark.stop()
