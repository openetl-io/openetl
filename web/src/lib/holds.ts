import { HoldWayOut_Action, type Hold } from "@/gen/openetl/api/v1/pipeline_pb";

export type HoldState = "held" | "pending" | "releasing" | "refused" | "discarded";

export const holdStateLabel: Record<HoldState, string> = {
  held: "Held",
  pending: "Pending",
  releasing: "Releasing",
  refused: "Release refused",
  discarded: "Discarded",
};

export const holdStateHint: Record<HoldState, string> = {
  held: "Its values are kept until it's released or discarded",
  pending: "Released or discarded, waiting for the pipeline's next run",
  releasing: "Its held values are being patched in",
  refused: "Its last release didn't clear it, and isn't tried again",
  discarded: "Its values are dropped; the column stays as the destination has it",
};

/** A hold's state, as `openetl holds` shows it. */
export function holdState(h: Hold): HoldState {
  if (h.discarded) return "discarded";
  if (h.releasing) return "releasing";
  if (h.pending) return "pending";
  if (h.refused) return "refused";
  return "held";
}

export function heldWhat(h: Hold) {
  if (h.whole) return "Column";
  return h.cells === 1n ? "1 cell" : `${h.cells} cells`;
}

/** The note on a hold whose last release didn't clear it. */
export function refusalNote(h: Hold) {
  if (!h.refused) return undefined;
  const reason = h.refused.reason.replace(/\.$/, "");
  return `Its last release didn't clear it: ${reason}. Release it again once that's fixed.`;
}

const subcommands: Partial<Record<HoldWayOut_Action, string>> = {
  [HoldWayOut_Action.RELEASE]: "release",
  [HoldWayOut_Action.RELEASE_NEW_VERSION]: "release --new-version",
  [HoldWayOut_Action.DISCARD]: "discard",
};

/** The CLI command for a way out of a hold; workspace and pipeline are IDs. */
export function holdCommand(action: HoldWayOut_Action, workspace: string, pipeline: string, h: Hold) {
  const sub = subcommands[action];
  if (!sub) return undefined;
  const ws = workspace === "default" ? "" : `--workspace ${workspace} `;
  return `openetl holds ${sub} ${ws}${pipeline} ${h.stream} ${h.column}`;
}
