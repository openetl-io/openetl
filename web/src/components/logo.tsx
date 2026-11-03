import { useId } from "react";
import { cn } from "@/lib/utils";

// The Soft mark: the O is the source, and three streams leave it. With
// motion "hover", the streams slide out again when a parent .group is
// hovered or focused; with "intro", they slide out once. On the dark theme
// the tile turns light and the O and streams ink, so the tile doesn't vanish.
const tile =
  "M332.8 0c62.73 0 94.09 0 118.05 12.21a112 112 0 0 1 48.95 48.95c12.21 23.96 12.21 55.32 12.21 118.05L512 332.8c0 62.73 0 94.09-12.21 118.05a112 112 0 0 1-48.95 48.95c-23.96 12.21-55.32 12.21-118.05 12.21L179.2 512c-62.73 0-94.09 0-118.05-12.21a112 112 0 0 1-48.95-48.95C0 426.89 0 395.53 0 332.8L0 179.2c0-62.73 0-94.09 12.21-118.05a112 112 0 0 1 48.95-48.95C85.11 0 116.47 0 179.2 0Z";
const ring = "M38 256A150 150 0 1 0 338 256A150 150 0 1 0 38 256ZM98 256A90 90 0 1 0 278 256A90 90 0 1 0 98 256Z";
const streams = [
  { d: "M188 106H444A30 30 0 0 1 444 166H188Z", clip: [188, 105, 324, 62] },
  { d: "M308 226H412A30 30 0 0 1 412 286H308Z", clip: [308, 225, 204, 62] },
  { d: "M188 346H444A30 30 0 0 1 444 406H188Z", clip: [188, 345, 324, 62] },
] as const;

export function Logo({ className, motion = "hover" }: { className?: string; motion?: "hover" | "intro" | "none" }) {
  const id = useId().replaceAll(":", "");
  return (
    <svg
      viewBox="0 0 512 512"
      aria-hidden
      className={cn("oe-logo shrink-0", motion === "hover" && "oe-hover", motion === "intro" && "oe-intro", className)}
    >
      <defs>
        {streams.map((s, i) => (
          <clipPath key={i} id={`${id}-${i}`}>
            <rect x={s.clip[0]} y={s.clip[1]} width={s.clip[2]} height={s.clip[3]} />
          </clipPath>
        ))}
      </defs>
      <path d={tile} className="fill-[#141416] dark:fill-[#F4F4F5]" />
      <path d={ring} className="fill-[#F4F4F5] dark:fill-[#141416]" fillRule="evenodd" />
      {streams.map((s, i) => (
        <g key={i} clipPath={`url(#${id}-${i})`}>
          <path className={`oe-s${i + 1} fill-[#F4F4F5] dark:fill-[#141416]`} d={s.d} />
        </g>
      ))}
    </svg>
  );
}
