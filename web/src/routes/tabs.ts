export const tabs = ["overview", "syncs", "schemas", "checkpoints", "versions"] as const;
export type Tab = (typeof tabs)[number];
