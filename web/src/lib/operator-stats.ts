import type { DiskReads, OperatorStats, StateRequests } from "@/gen/openetl/api/v1/pipeline_pb";
import { timestampDate, type Timestamp } from "@bufbuild/protobuf/wkt";
import { duration, full } from "@/lib/format";

// A stateful operator's numbers at a checkpoint, as the SQL tab shows them.

/** 0 B, 812 B, 4.2 KiB, 12 MiB, 1.5 GiB */
export function stateBytes(n: bigint) {
  const v = Number(n);
  const units = ["B", "KiB", "MiB", "GiB", "TiB"];
  let i = 0;
  let x = v;
  while (x >= 1024 && i < units.length - 1) {
    x /= 1024;
    i++;
  }
  return `${i === 0 ? x : x.toLocaleString("en", { maximumFractionDigits: 1 })} ${units[i]}`;
}

/** Where the operator's watermark was, and how far behind the checkpoint's commit. */
export function watermark(s: OperatorStats, committed?: Timestamp): { at: string; behind: string } {
  if (s.endOfInput) return { at: "End of input", behind: "Every window closed" };
  if (!s.watermark) return { at: "None", behind: "Part of the stream has had no rows, or it's outside years 1 to 9999" };
  const at = timestampDate(s.watermark);
  const behind = committed ? timestampDate(committed).getTime() - at.getTime() : 0;
  return { at: at.toISOString().replace(/\.\d{3}Z$/, "Z"), behind: behind > 0 ? `${duration(behind)} behind the commit` : "At the commit" };
}

/** What the operator wrote, by op: "30 new, 10 updated, 2 deleted". */
export function written(s: OperatorStats) {
  return `${full(s.inserts)} new, ${full(s.updates)} updated, ${full(s.deletes)} deleted`;
}

/** Requests of one kind, or reads from disk: "3 (1.2 MiB)", or "0". */
export function requests(r?: StateRequests | DiskReads) {
  if (!r || r.count === 0n) return "0";
  return `${full(r.count)} (${stateBytes(r.bytes)})`;
}
