import { Table, TableBody, TableCell, TableHead, TableHeader, TableRow } from "@/components/ui/table";
import type { SyncAttempt } from "@/gen/openetl/api/v1/sync_pb";
import { attemptUse, bytes, cpuTime, laneShares, sizeAmounts, sizeLabel, sizeReasonLabel } from "@/lib/sizes";

/** A sync's attempts: what each was sized at, why, what it used, and how
 * busy its lanes were. */
export function Attempts({ attempts }: { attempts: SyncAttempt[] }) {
  return (
    <section className="grid gap-2" aria-labelledby="attempts-h">
      <h2 id="attempts-h" className="text-base font-semibold">
        Attempts
      </h2>
      <div className="rounded-lg border">
        <Table>
          <TableHeader>
            <TableRow>
              <TableHead>Attempt</TableHead>
              <TableHead>Size</TableHead>
              <TableHead>Why</TableHead>
              <TableHead className="text-right">CPU</TableHead>
              <TableHead className="text-right">Peak memory</TableHead>
              <TableHead className="text-right">Lanes</TableHead>
              <TableHead className="text-right">Busy</TableHead>
              <TableHead>Failed with</TableHead>
            </TableRow>
          </TableHeader>
          <TableBody>
            {attempts.map((a) => {
              const use = attemptUse(a);
              const lanes = laneShares(a);
              return (
                <TableRow key={a.attempt}>
                  <TableCell className="tabular-nums">{a.attempt}</TableCell>
                  <TableCell title={sizeAmounts(a.size)}>{sizeLabel(a.size)}</TableCell>
                  <TableCell className="text-muted-foreground">{sizeReasonLabel[a.sizeReason]}</TableCell>
                  <TableCell className="text-right tabular-nums">{a.usage ? cpuTime(use.cpu) : "—"}</TableCell>
                  <TableCell className="text-right tabular-nums">{a.usage ? bytes(use.memory) : "—"}</TableCell>
                  <TableCell className="text-right tabular-nums">{lanes?.lanes ?? "—"}</TableCell>
                  <TableCell
                    className="text-right tabular-nums"
                    title={lanes && `Waiting for the destination ${lanes.backpressured}, for rows ${lanes.idle}`}
                  >
                    {lanes?.busy ?? "—"}
                  </TableCell>
                  <TableCell className="font-mono text-[0.8125rem]">{a.code}</TableCell>
                </TableRow>
              );
            })}
          </TableBody>
        </Table>
      </div>
    </section>
  );
}
