import json
import unittest

from openetl_spark import PROTOCOL, EpochContext
from openetl_spark.transform import Driver, _run

ROOT = "mem://root/"
USERS = {"type": "struct", "fields": [{"name": "id", "type": "long", "nullable": True, "metadata": {}}]}


class FakeFS:
    """The attempt's directory, in memory: files by path, with their text and modification time."""

    def __init__(self):
        self.data = {}

    def put(self, rel, text="", mtime=0):
        self.data[rel] = (text, mtime)

    def json(self, rel):
        return json.loads(self.data[rel][0])

    def uri(self, rel):
        return ROOT + rel

    def exists(self, rel):
        return rel in self.data or any(k.startswith(rel + "/") for k in self.data)

    def modified(self, rel):
        f = self.data.get(rel)
        return None if f is None else f[1]

    def dirs(self):
        return sorted({k.split("/", 1)[0] for k in self.data if "/" in k})

    def files(self, rel):
        prefix = rel + "/"
        return [k[len(prefix) :] for k in self.data if k.startswith(prefix) and "/" not in k[len(prefix) :]]

    def read(self, rel):
        return self.data[rel][0]

    def write(self, rel, text):
        self.put(rel, text)


class Frame:
    def __init__(self, fs, what):
        self.fs = fs
        self.what = what

    @property
    def write(self):
        return Writer(self)


class Writer:
    def __init__(self, frame):
        self.frame = frame

    def mode(self, mode):
        assert mode == "overwrite"
        return self

    def parquet(self, uri):
        assert uri.startswith(ROOT)
        rel = uri[len(ROOT) :]
        fs = self.frame.fs
        fs.put(rel + "/part-00001.parquet", repr(self.frame.what))
        fs.put(rel + "/part-00000.parquet", repr(self.frame.what))
        fs.put(rel + "/.part-00000.parquet.crc")
        fs.put(rel + "/_SUCCESS")


class FakeSpark:
    def __init__(self, fs, conf=None):
        self.fs = fs
        self.conf = conf or {}
        self._schema = None

    @property
    def read(self):
        self._schema = None
        return self

    def schema(self, s):
        self._schema = s
        return self

    def parquet(self, *uris):
        return Frame(self.fs, ("parquet", self._schema, uris))

    def createDataFrame(self, rows, schema):
        assert rows == []
        return Frame(self.fs, ("empty", schema))


class Clock:
    def __init__(self):
        self.now = 0.0

    def __call__(self):
        return self.now

    def sleep(self, s):
        self.now += s


def ready(fs, n, inputs, outputs, generation=1, version=1):
    fs.put(f"{n}/in/_ready.json", json.dumps({"version": version, "generation": generation, "epoch": n, "inputs": inputs, "outputs": outputs}))


def driver(fs, transform, clock=None):
    clock = clock or Clock()
    # Without _alive, a loop that never sees _end still stops.
    return Driver(FakeSpark(fs), fs, transform, poll=1, stale=30, clock=clock, sleep=clock.sleep, schema=lambda d: d)


def passthrough(name="a", end_after=None):
    def t(spark, inputs, ctx):
        if end_after is not None and ctx.epoch == end_after:
            spark.fs.put("_end")
        return {o: inputs[name] for o in ctx.outputs}

    return t


class TransformTest(unittest.TestCase):
    def test_transforms_an_epoch(self):
        fs = FakeFS()
        ready(
            fs,
            7,
            [
                {"name": "orders", "schema": USERS, "files": ["in/orders/part-00000.parquet"], "rows": 2},
                {"name": "users", "schema": USERS, "files": [], "rows": 0},
                {"name": "events", "schema": None, "files": [], "rows": 0},
            ],
            ["enriched", "totals"],
            generation=3,
        )
        seen = []

        def t(spark, inputs, ctx):
            seen.append((list(inputs), ctx))
            self.assertEqual(inputs["orders"].what, ("parquet", USERS, (ROOT + "7/in/orders/part-00000.parquet",)))
            self.assertEqual(inputs["users"].what, ("empty", USERS))
            self.assertEqual(inputs["events"].what, ("empty", {"type": "struct", "fields": []}))
            fs.put("_end")
            return {"totals": inputs["users"], "enriched": inputs["orders"]}

        driver(fs, t).run()
        self.assertEqual(seen, [(["orders", "users", "events"], EpochContext(generation=3, epoch=7, outputs=("enriched", "totals")))])
        done = fs.json("7/out/_done.json")
        self.assertEqual(
            done,
            {
                "version": 1,
                "epoch": 7,
                "outputs": [
                    {"name": "enriched", "files": ["out/0/part-00000.parquet", "out/0/part-00001.parquet"]},
                    {"name": "totals", "files": ["out/1/part-00000.parquet", "out/1/part-00001.parquet"]},
                ],
            },
        )
        self.assertIn("parquet", fs.read("7/out/0/part-00000.parquet"))
        self.assertIn("empty", fs.read("7/out/1/part-00000.parquet"))
        self.assertFalse(fs.exists("_failed.json"))

    def test_takes_ready_epochs_in_order_and_skips_done_ones(self):
        fs = FakeFS()
        for n in (9, 2, 5):
            ready(fs, n, [{"name": "a", "schema": USERS, "files": []}], ["o"])
        fs.put("2/out/_done.json", '{"from": "an earlier driver"}')
        fs.put("11/in/a/part-00000.parquet")
        fs.put("code/abc/enrich.py")
        epochs = []

        def t(spark, inputs, ctx):
            epochs.append(ctx.epoch)
            return passthrough(end_after=9)(spark, inputs, ctx)

        driver(fs, t).run()
        self.assertEqual(epochs, [5, 9])
        self.assertEqual(fs.json("2/out/_done.json"), {"from": "an earlier driver"})
        self.assertTrue(fs.exists("5/out/_done.json"))
        self.assertFalse(fs.exists("11/out/_done.json"))

    def test_stops_at_end(self):
        fs = FakeFS()
        ready(fs, 1, [{"name": "a", "schema": USERS, "files": []}], ["o"])
        fs.put("_end")

        def t(spark, inputs, ctx):
            raise AssertionError("transformed after _end")

        driver(fs, t).run()
        self.assertFalse(fs.exists("1/out/_done.json"))

    def test_stops_once_alive_is_stale(self):
        fs = FakeFS()
        fs.put("_alive", mtime=1000)
        clock = Clock()
        driver(fs, passthrough(), clock).run()
        self.assertGreaterEqual(clock.now, 30)
        self.assertLess(clock.now, 40)

    def test_keeps_going_while_alive_changes(self):
        fs = FakeFS()
        clock = Clock()
        fs.put("_alive", mtime=1)

        def sleep(s):
            clock.sleep(s)
            if clock.now >= 100:
                fs.put("_end")
            elif clock.now % 20 == 0:
                fs.put("_alive", mtime=fs.modified("_alive") + 1)

        d = Driver(FakeSpark(fs), fs, passthrough(), poll=1, stale=30, clock=clock, sleep=sleep, schema=lambda d: d)
        d.run()
        self.assertEqual(clock.now, 100)

    def test_writes_the_users_failure(self):
        fs = FakeFS()
        ready(fs, 4, [{"name": "a", "schema": USERS, "files": []}], ["o"])

        def t(spark, inputs, ctx):
            raise RuntimeError("boom")

        with self.assertRaisesRegex(RuntimeError, "boom"):
            driver(fs, t).run()
        failed = fs.json("_failed.json")
        self.assertEqual((failed["version"], failed["epoch"], failed["error"]), (1, 4, "RuntimeError: boom"))
        self.assertIn("test_transform", failed["trace"])
        self.assertFalse(fs.exists("4/out/_done.json"))

    def test_fails_a_missing_or_undeclared_output(self):
        fs = FakeFS()
        ready(fs, 1, [{"name": "a", "schema": USERS, "files": []}], ["o", "p"])
        with self.assertRaises(ValueError):
            driver(fs, lambda spark, inputs, ctx: {"o": inputs["a"]}).run()
        self.assertIn("returned none for ['p']", fs.json("_failed.json")["error"])
        with self.assertRaises(ValueError):
            driver(fs, lambda spark, inputs, ctx: {"o": inputs["a"], "p": inputs["a"], "q": inputs["a"]}).run()
        self.assertIn("returned undeclared ['q']", fs.json("_failed.json")["error"])

    def test_refuses_another_version(self):
        fs = FakeFS()
        ready(fs, 1, [{"name": "a", "schema": USERS, "files": []}], ["o"], version=2)
        with self.assertRaises(Exception):
            driver(fs, passthrough()).run()
        self.assertIn("version 2", fs.json("_failed.json")["error"])

    def test_refuses_another_protocol(self):
        fs = FakeFS()
        clock = Clock()
        options = dict(poll=1, stale=30, clock=clock, sleep=clock.sleep, schema=lambda d: d)
        with self.assertRaises(Exception):
            _run(FakeSpark(fs, {PROTOCOL: "2"}), fs, passthrough(), **options)
        failed = fs.json("_failed.json")
        self.assertEqual(failed["epoch"], 0)
        self.assertIn("protocol 2", failed["error"])

        del fs.data["_failed.json"]
        fs.put("_end")
        _run(FakeSpark(fs, {PROTOCOL: "1.3"}), fs, passthrough(), **options)
        self.assertFalse(fs.exists("_failed.json"))


if __name__ == "__main__":
    unittest.main()
