import json
import uuid
from datetime import datetime
from pathlib import Path
from typing import Any

# Categories constants
EXPLAIN_PLAN = "EXPLAIN_PLAN"
LLM_PROMPT = "LLM_PROMPT"
LLM_RESPONSE = "LLM_RESPONSE"
RECOMMENDATIONS = "RECOMMENDATIONS"
HYPOPG_VALIDATION = "HYPOPG_VALIDATION"
LLM_ROUTING = "LLM_ROUTING"
DETECTOR_SUPPRESSIONS = "DETECTOR_SUPPRESSIONS"


class DebugStore:
    def __init__(self, store_path: Path):
        self.store_path = store_path

    def new_run_id(self) -> str:
        """Generate a new run ID: YYYYMMDD-HHMMSS-{short_uuid}"""
        now = datetime.now()
        timestamp = now.strftime("%Y%m%d-%H%M%S")
        short_id = str(uuid.uuid4())[:8]
        return f"{timestamp}-{short_id}"

    def save(self, run_id: str, category: str, data: dict[str, Any]) -> Path:
        """Writes to {STORE}/{YYYY-MM-DD}/{run_id}/{category}.json"""
        # Use current date for partitioning as requested
        date_folder = datetime.now().strftime("%Y-%m-%d")
        dir_path = self.store_path / date_folder / run_id
        dir_path.mkdir(parents=True, exist_ok=True)

        file_path = dir_path / f"{category}.json"
        with open(file_path, "w") as f:
            json.dump(data, f, indent=2)

        return file_path

    def load(self, run_id: str, category: str) -> dict[str, Any]:
        """Loads from {STORE}/*/{run_id}/{category}.json"""
        matches = list(self.store_path.glob(f"*/{run_id}/{category}.json"))
        if not matches:
            raise FileNotFoundError(f"No debug artifact for {run_id}/{category}")

        with open(matches[0]) as f:
            return json.load(f)

    def list_runs(self, limit: int = 20) -> list[dict[str, Any]]:
        """List recent runs: date, run_id, query snippet"""
        runs = []
        if not self.store_path.exists():
            return []

        # store_path / YYYY-MM-DD / run_id
        # Sort date directories descending
        for date_dir in sorted(
            [d for d in self.store_path.iterdir() if d.is_dir()],
            key=lambda x: x.name,
            reverse=True,
        ):
            # Sort run directories descending
            for run_dir in sorted(
                [r for r in date_dir.iterdir() if r.is_dir()],
                key=lambda x: x.name,
                reverse=True,
            ):
                run_id = run_dir.name
                query_snippet = "N/A"
                try:
                    # Try to find a snippet in specific order
                    snippet_found = False
                    for category in [EXPLAIN_PLAN, LLM_PROMPT]:
                        file = run_dir / f"{category}.json"
                        if file.exists():
                            data = json.loads(file.read_text())
                            if category == EXPLAIN_PLAN and "query" in data:
                                query = data["query"]
                                query_snippet = (
                                    (query[:50] + "...") if len(query) > 50 else query
                                )
                                snippet_found = True
                                break
                            elif category == LLM_PROMPT and "prompt" in data:
                                prompt = str(data["prompt"])
                                query_snippet = (
                                    (prompt[:50] + "...")
                                    if len(prompt) > 50
                                    else prompt
                                )
                                snippet_found = True
                                break

                    if not snippet_found:
                        # Fallback to any json if still not found
                        for file in run_dir.glob("*.json"):
                            data = json.loads(file.read_text())
                            if "query" in data:
                                query = data["query"]
                                query_snippet = (
                                    (query[:50] + "...") if len(query) > 50 else query
                                )
                                break
                except Exception:
                    pass

                runs.append(
                    {
                        "date": date_dir.name,
                        "run_id": run_id,
                        "query_snippet": query_snippet,
                    }
                )

                if len(runs) >= limit:
                    break
            if len(runs) >= limit:
                break
        return runs

    def get_run_artifacts(self, run_id: str) -> dict[str, Any]:
        """Get all artifacts for a run"""
        run_dirs = list(self.store_path.glob(f"*/{run_id}"))
        if not run_dirs:
            raise FileNotFoundError(f"Run {run_id} not found")

        run_dir = run_dirs[0]
        artifacts = {}
        for file in sorted(run_dir.glob("*.json")):
            category = file.stem
            with open(file) as f:
                artifacts[category] = json.load(f)
        return artifacts
