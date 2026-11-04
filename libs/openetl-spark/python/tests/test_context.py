import unittest

from openetl_spark import CONF, Ref, SyncContext

# As the runner writes it (internal/spark.Context).
DOC = (
    '{"version":1,"pipeline":"p1","sync":"s1","destination":{"connector":"iceberg","namespace":"shop"},'
    '"after":{"epoch":3,"generation":1},"through":{"epoch":5,"generation":2},'
    '"streams":[{"name":"customers","rows":2},{"name":"orders","table":"shop_orders","rows":7}]}'
)


def summary(pipeline, epoch, generation):
    return {"openetl.pipeline": pipeline, "openetl.epoch": str(epoch), "openetl.generation": str(generation)}


class FakeSpark:
    """Just what SyncContext uses of a SparkSession."""

    def __init__(self, conf, files=None, snapshots=None):
        self.conf = conf
        self.read = self
        self._files = files or {}
        self._snapshots = snapshots or []
        self.queries = []

    def text(self, uri, wholetext=False):
        assert wholetext
        self._uri = uri
        return self

    def first(self):
        return [self._files[self._uri]]

    def sql(self, query):
        self.queries.append(query)
        return self

    def collect(self):
        return self._snapshots


class SyncContextTest(unittest.TestCase):
    def test_parses(self):
        c = SyncContext.parse(DOC)
        self.assertEqual((c.pipeline, c.sync, c.destination_connector, c.destination_namespace), ("p1", "s1", "iceberg", "shop"))
        self.assertEqual((c.after, c.through), (Ref(3, 1), Ref(5, 2)))
        self.assertEqual([(s.name, s.table, s.rows) for s in c.streams], [("customers", "", 2), ("orders", "shop_orders", 7)])

    def test_committed_is_the_syncs_records(self):
        c = SyncContext.parse(DOC)
        self.assertFalse(c.committed(summary("p1", 3, 1)), "the record the sync started after")
        self.assertTrue(c.committed(summary("p1", 3, 2)), "a later generation's epoch 3")
        self.assertTrue(c.committed(summary("p1", 5, 2)))
        self.assertFalse(c.committed(summary("p1", 5, 3)), "past the sync's last record")
        self.assertFalse(c.committed(summary("p2", 4, 1)), "another pipeline's")
        self.assertFalse(c.committed({}), "a snapshot openetl didn't write")

    def test_an_unknown_start_takes_everything_through_the_end(self):
        c = SyncContext.parse(DOC.replace('{"epoch":3,"generation":1}', "null"))
        self.assertIsNone(c.after)
        self.assertTrue(c.committed(summary("p1", 1, 1)))
        self.assertFalse(c.committed(summary("p1", 6, 1)))

    def test_refuses_another_version(self):
        with self.assertRaisesRegex(ValueError, "version 2"):
            SyncContext.parse(DOC.replace('"version":1', '"version":2'))

    def test_load_and_snapshots(self):
        spark = FakeSpark(
            {CONF: "s3a://lake/org/p1/_spark/context/s1/2.json"},
            files={"s3a://lake/org/p1/_spark/context/s1/2.json": DOC},
            snapshots=[(10, summary("p1", 4, 1)), (11, summary("p1", 9, 1)), (12, None)],
        )
        c = SyncContext.load(spark)
        self.assertEqual(c.sync, "s1")
        self.assertEqual(c.snapshots(spark, "lake.shop.orders"), [10])
        self.assertEqual(spark.queries, ["SELECT snapshot_id, summary FROM lake.shop.orders.snapshots"])
        with self.assertRaisesRegex(RuntimeError, CONF):
            SyncContext.load(FakeSpark({}))


if __name__ == "__main__":
    unittest.main()
