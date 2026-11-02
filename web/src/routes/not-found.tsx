import { Link } from "@tanstack/react-router";
import { Empty } from "@/components/empty";

export function NotFound() {
  return (
    <div className="mx-auto max-w-xl p-10">
      <Empty title="There's no page here">
        <Link to="/" className="text-signal underline-offset-4 hover:underline">
          Go to your pipelines
        </Link>
      </Empty>
    </div>
  );
}
