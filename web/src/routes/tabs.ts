export const tabs = ["overview", "sql", "syncs", "schemas", "checkpoints", "versions"] as const;
export type Tab = (typeof tabs)[number];
