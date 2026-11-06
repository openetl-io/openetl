"""The driver of a pipeline's PySpark transform.

The user's file is the application's main, and calls `run` with its
transform, once per sync attempt:

    def transform(spark, inputs, ctx):
        return {"orders_enriched": inputs["orders"].join(...)}

    if __name__ == "__main__":
        openetl_spark.run(transform)

`run` transforms each epoch the runner stages below ROOT, in order, until the
runner writes _end or stops rewriting _alive. Below the root, {N}/in/_ready.json
lists epoch N's inputs; the driver writes output k to {N}/out/{k}/ and lists
the files in {N}/out/_done.json. A failure of the user's code goes to
_failed.json.
"""

from __future__ import annotations

import json
import logging
import time
import traceback
from dataclasses import dataclass
from typing import Any, Callable, Dict, List, Mapping, Optional, Set, Tuple

__all__ = ["PROTOCOL", "PROTOCOL_VERSION", "ROOT", "EpochContext", "run"]

#: The Spark property naming the attempt's directory, ending in a slash.
ROOT = "spark.openetl.root"
#: The Spark property naming the protocol the runner speaks.
PROTOCOL = "spark.openetl.protocol"
#: The protocol's major version this library speaks.
PROTOCOL_VERSION = 1

POLL = 1.0
STALE = 300.0

log = logging.getLogger("openetl_spark")


@dataclass(frozen=True)
class EpochContext:
    """The epoch a transform is called for."""

    #: The sync attempt's generation.
    generation: int
    epoch: int
    #: The outputs the transform returns, by name, as the pipeline declares them.
    outputs: Tuple[str, ...]


class HadoopFileSystem:
    """The attempt's directory, through the JVM's Hadoop file systems.

    Paths are relative to the root.
    """

    def __init__(self, spark: Any, root: str):
        self._jvm = spark._jvm
        self.root = root if root.endswith("/") else root + "/"
        self._fs = self._path("").getFileSystem(spark._jsc.hadoopConfiguration())

    def _path(self, rel: str) -> Any:
        return self._jvm.org.apache.hadoop.fs.Path(self.root + rel)

    def uri(self, rel: str) -> str:
        return self.root + rel

    def exists(self, rel: str) -> bool:
        return bool(self._fs.exists(self._path(rel)))

    def modified(self, rel: str) -> Optional[int]:
        """The modification time, or None if there's no such file."""
        try:
            return int(self._fs.getFileStatus(self._path(rel)).getModificationTime())
        except Exception:  # FileNotFoundException, through Py4J
            return None

    def _list(self, rel: str) -> List[Any]:
        p = self._path(rel)
        if not self._fs.exists(p):
            return []
        return list(self._fs.listStatus(p))

    def dirs(self) -> List[str]:
        """The root's subdirectories' names."""
        return [s.getPath().getName() for s in self._list("") if s.isDirectory()]

    def files(self, rel: str) -> List[str]:
        """The names of the files in a directory; none if it doesn't exist."""
        return [s.getPath().getName() for s in self._list(rel) if s.isFile()]

    def read(self, rel: str) -> str:
        stream = self._fs.open(self._path(rel))
        try:
            return self._jvm.org.apache.commons.io.IOUtils.toString(stream, "UTF-8")
        finally:
            stream.close()

    def write(self, rel: str, text: str) -> None:
        out = self._fs.create(self._path(rel), True)
        try:
            out.write(bytearray(text.encode("utf-8")))
        finally:
            out.close()


def _spark_schema(d: Mapping[str, Any]) -> Any:
    from pyspark.sql.types import StructType

    return StructType.fromJson(d)


class _ProtocolError(Exception):
    pass


class Driver:
    """The loop that `run` drives; a test drives it with a fake file system."""

    def __init__(
        self,
        spark: Any,
        fs: Any,
        transform: Callable[[Any, Dict[str, Any], EpochContext], Mapping[str, Any]],
        poll: float = POLL,
        stale: float = STALE,
        clock: Callable[[], float] = time.monotonic,
        sleep: Callable[[float], None] = time.sleep,
        schema: Callable[[Mapping[str, Any]], Any] = _spark_schema,
    ):
        self.spark = spark
        self.fs = fs
        self.transform = transform
        self.poll = poll
        self.stale = stale
        self.clock = clock
        self.sleep = sleep
        self.schema = schema
        self.finished: Set[int] = set()

    def run(self) -> None:
        """Transforms each epoch as it's ready, until _end, or until _alive is stale.

        A failure of the user's code is written to _failed.json and raised.
        """
        looked, alive, changed = False, None, 0.0
        while True:
            if self.fs.exists("_end"):
                log.info("the runner ended the attempt")
                return
            modified = self.fs.modified("_alive")
            now = self.clock()
            if not looked or modified != alive:
                looked, alive, changed = True, modified, now
            elif now - changed >= self.stale:
                log.warning("_alive hasn't changed for %.0f s, so the runner is gone; exiting", now - changed)
                return
            n = self._next()
            if n is not None:
                self._epoch(n)
                self.finished.add(n)
                continue
            self.sleep(self.poll)

    def _next(self) -> Optional[int]:
        epochs = sorted(int(d) for d in self.fs.dirs() if d.isdigit() and len(d) <= 18)
        for n in epochs:
            if n not in self.finished and self.fs.exists(f"{n}/in/_ready.json") and not self.fs.exists(f"{n}/out/_done.json"):
                return n
        return None

    def _epoch(self, n: int) -> None:
        log.info("transforming epoch %d", n)
        try:
            generation, inputs, outputs = self._ready(n)
        except (_ProtocolError, ValueError, KeyError, TypeError) as e:
            self.fail(n, e)
            raise
        frames = {name: self._input(n, schema, files) for name, schema, files in inputs}
        ctx = EpochContext(generation=generation, epoch=n, outputs=tuple(outputs))
        try:
            out = self.transform(self.spark, frames, ctx)
            _check_outputs(outputs, out)
            for k, name in enumerate(outputs):
                out[name].write.mode("overwrite").parquet(self.fs.uri(f"{n}/out/{k}"))
        except Exception as e:
            self.fail(n, e)
            raise
        done = {
            "version": PROTOCOL_VERSION,
            "epoch": n,
            "outputs": [{"name": name, "files": [f"out/{k}/{f}" for f in self._parts(f"{n}/out/{k}")]} for k, name in enumerate(outputs)],
        }
        self.fs.write(f"{n}/out/_done.json", json.dumps(done))
        log.info("epoch %d done", n)

    def _ready(self, n: int) -> Tuple[int, List[Tuple[str, Any, List[str]]], List[str]]:
        doc = json.loads(self.fs.read(f"{n}/in/_ready.json"))
        version = doc.get("version")
        if version != PROTOCOL_VERSION:
            raise _ProtocolError(f"epoch {n}'s _ready.json is version {version}, and this openetl-spark reads {PROTOCOL_VERSION}")
        if doc.get("epoch") != n:
            raise _ProtocolError(f"epoch {n}'s _ready.json is for epoch {doc.get('epoch')}")
        inputs = []
        for i in doc.get("inputs") or []:
            schema = i.get("schema")
            inputs.append((i["name"], None if schema is None else self.schema(schema), list(i.get("files") or [])))
        return int(doc.get("generation", 0)), inputs, [str(o) for o in doc.get("outputs") or []]

    def _input(self, n: int, schema: Any, files: List[str]) -> Any:
        if files:
            reader = self.spark.read
            if schema is not None:
                reader = reader.schema(schema)
            return reader.parquet(*[self.fs.uri(f"{n}/{f}") for f in files])
        return self.spark.createDataFrame([], schema if schema is not None else self.schema({"type": "struct", "fields": []}))

    def _parts(self, rel: str) -> List[str]:
        return sorted(f for f in self.fs.files(rel) if f.startswith("part-") and f.endswith(".parquet"))

    def fail(self, epoch: int, e: BaseException) -> None:
        """Writes the failure to _failed.json."""
        doc = {
            "version": PROTOCOL_VERSION,
            "epoch": epoch,
            "error": f"{type(e).__name__}: {e}",
            "trace": "".join(traceback.format_exception(type(e), e, e.__traceback__)),
        }
        try:
            self.fs.write("_failed.json", json.dumps(doc))
        except Exception:
            log.exception("writing _failed.json failed")


def _check_outputs(declared: List[str], out: Optional[Mapping[str, Any]]) -> None:
    out = out or {}
    missing = [name for name in declared if out.get(name) is None]
    extra = [name for name in out if name not in declared]
    if missing or extra:
        msg = f"the transform's outputs are {declared}"
        if missing:
            msg += f", and it returned none for {missing}"
        if extra:
            msg += f"{', and also' if missing else ', and'} returned undeclared {extra}"
        raise ValueError(msg)


def _check_protocol(protocol: str) -> None:
    if protocol.split(".", 1)[0] != str(PROTOCOL_VERSION):
        raise _ProtocolError(f"the runner speaks protocol {protocol or '(none)'}, and this openetl-spark speaks {PROTOCOL_VERSION}")


def _run(spark: Any, fs: Any, transform: Callable[..., Mapping[str, Any]], **options: Any) -> None:
    driver = Driver(spark, fs, transform, **options)
    try:
        _check_protocol(spark.conf.get(PROTOCOL, ""))
    except _ProtocolError as e:
        driver.fail(0, e)
        raise
    driver.run()


def run(transform: Callable[[Any, Dict[str, Any], EpochContext], Mapping[str, Any]]) -> None:
    """Runs the pipeline's transform, then stops the session.

    `transform(spark, inputs, ctx)` gets a DataFrame for every input stream by
    name, empty if the stream has no rows in the epoch, and returns a
    DataFrame for each of ctx.outputs. A failure is raised, so the application
    fails.
    """
    from pyspark.sql import SparkSession

    spark = SparkSession.builder.getOrCreate()
    try:
        root = spark.conf.get(ROOT, "")
        if not root:
            raise RuntimeError(f"{ROOT} isn't set: this application isn't an openetl transform")
        _run(spark, HadoopFileSystem(spark, root), transform)
    finally:
        spark.stop()
