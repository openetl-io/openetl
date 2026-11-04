"""What a post-sync PySpark application learns of the sync that ran before it.

    from openetl_spark import SyncContext

    ctx = SyncContext.load(spark)
    for snapshot in ctx.snapshots(spark, "lake.shop.orders"):
        ...
"""

from __future__ import annotations

import json
from dataclasses import dataclass
from typing import Any, List, Mapping, Optional

__all__ = ["CONF", "VERSION", "Ref", "Stream", "SyncContext"]

#: The Spark property that names the context document.
CONF = "spark.openetl.context"

#: The document's protocol version this library reads.
VERSION = 1


@dataclass(frozen=True, order=True)
class Ref:
    """A checkpoint record: an epoch, and the generation that wrote it."""

    epoch: int
    generation: int

    @classmethod
    def _of(cls, d: Optional[Mapping[str, Any]]) -> Optional["Ref"]:
        if d is None:
            return None
        return cls(int(d.get("epoch", 0)), int(d.get("generation", 0)))


@dataclass(frozen=True)
class Stream:
    """One of the pipeline's streams, with what the sync read of it."""

    name: str
    #: The destination's table; empty if the checkpoint log doesn't name it.
    table: str
    #: The rows the source emitted in the sync, quarantined ones included.
    rows: int


@dataclass(frozen=True)
class SyncContext:
    pipeline: str
    sync: str
    #: The destination's connector, such as postgres or iceberg.
    destination_connector: str
    #: The schema or namespace the pipeline writes to; empty if it has none.
    destination_namespace: str
    #: The record the sync started after; None if the checkpoint log doesn't say.
    after: Optional[Ref]
    #: The last record the sync committed; equal to after if it committed none.
    through: Ref
    streams: List[Stream]

    @classmethod
    def load(cls, spark: Any) -> "SyncContext":
        """Reads the document that CONF names, through Spark's file systems."""
        uri = spark.conf.get(CONF, "")
        if not uri:
            raise RuntimeError(f"{CONF} isn't set: this application isn't an openetl post-sync step")
        return cls.parse(spark.read.text(uri, wholetext=True).first()[0])

    @classmethod
    def parse(cls, text: str) -> "SyncContext":
        d = json.loads(text)
        version = d.get("version")
        if version != VERSION:
            raise ValueError(f"the sync's context is version {version}, and this openetl-spark reads {VERSION}")
        dst = d.get("destination") or {}
        return cls(
            pipeline=d.get("pipeline", ""),
            sync=d.get("sync", ""),
            destination_connector=dst.get("connector", ""),
            destination_namespace=dst.get("namespace", ""),
            after=Ref._of(d.get("after")),
            through=Ref._of(d.get("through")) or Ref(0, 0),
            streams=[Stream(s.get("name", ""), s.get("table", ""), int(s.get("rows", 0))) for s in d.get("streams") or []],
        )

    def committed(self, summary: Optional[Mapping[str, str]]) -> bool:
        """Whether this sync committed the snapshot with this summary.

        It's one of the pipeline's whose epoch is after `after` and through
        `through`. destination-iceberg writes the openetl.pipeline,
        openetl.generation and openetl.epoch keys it reads.
        """
        if not summary or summary.get("openetl.pipeline") != self.pipeline:
            return False
        try:
            r = Ref(int(summary["openetl.epoch"]), int(summary["openetl.generation"]))
        except (KeyError, ValueError):
            return False
        return (self.after is None or r > self.after) and r <= self.through

    def snapshots(self, spark: Any, table: str) -> List[int]:
        """The IDs of an Iceberg table's snapshots this sync committed, from its snapshots table."""
        rows = spark.sql(f"SELECT snapshot_id, summary FROM {table}.snapshots").collect()
        return [r[0] for r in rows if self.committed(r[1])]
