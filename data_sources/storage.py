"""
storage.py
----------
Lightweight file-based storage for the MVP pipeline.

Handles:
  - Writing/reading city records to JSON + CSV
  - Checkpointing (resume interrupted runs)
  - Deduplication by wikidata_id
  - Audit log per run

For the MVP we skip a database dependency to keep the pipeline self-contained.
Once you have clean data, import it into Postgres using the schema in schema.sql.
"""

import json
import csv
import logging
import hashlib
from datetime import datetime, timezone
from pathlib import Path
from typing import Optional

logger = logging.getLogger(__name__)


class CityStorage:
    """
    File-based storage for city pipeline output.

    Directory layout:
        data/
          cities_raw.json        — full records with all fields
          cities_raw.csv         — flat summary for quick inspection
          checkpoint.json        — tracks which wikidata_ids are processed
          run_log.jsonl          — append-only audit log
    """

    CITY_FIELDS_CSV = [
        "wikidata_id", "name", "country", "country_code",
        "lat", "lon", "population", "continent",
        "wiki_title", "wiki_url", "description",
        "extract_length", "needs_manual_review", "review_reason",
    ]

    def __init__(self, data_dir: str = "data"):
        self.data_dir = Path(data_dir)
        self.data_dir.mkdir(parents=True, exist_ok=True)

        self.json_path       = self.data_dir / "cities_raw.json"
        self.csv_path        = self.data_dir / "cities_raw.csv"
        self.checkpoint_path = self.data_dir / "checkpoint.json"
        self.log_path        = self.data_dir / "run_log.jsonl"

        self._checkpoint: dict = self._load_checkpoint()

    # ------------------------------------------------------------------
    # City record I/O
    # ------------------------------------------------------------------

    def save_cities(self, cities: list[dict]) -> int:
        """
        Save / merge city records.

        - Merges with any existing data (deduplicates by wikidata_id)
        - Writes both JSON and CSV
        - Updates checkpoint

        Returns number of new records added.
        """
        existing = self.load_cities()
        existing_ids = {c["wikidata_id"] for c in existing if c.get("wikidata_id")}

        new_cities = [c for c in cities if c.get("wikidata_id") not in existing_ids]
        all_cities = existing + new_cities

        # Write JSON
        with open(self.json_path, "w", encoding="utf-8") as f:
            json.dump(all_cities, f, ensure_ascii=False, indent=2, default=str)

        # Write CSV (flat summary — useful for quick inspection in Excel/pandas)
        self._write_csv(all_cities)

        # Update checkpoint
        for c in new_cities:
            if c.get("wikidata_id"):
                self._checkpoint["processed_ids"][c["wikidata_id"]] = {
                    "name": c.get("name"),
                    "saved_at": datetime.now(timezone.utc).isoformat(),
                }
        self._save_checkpoint()

        logger.info("Saved %d new cities (%d total)", len(new_cities), len(all_cities))
        return len(new_cities)

    def load_cities(self) -> list[dict]:
        """Load all saved city records from JSON."""
        if not self.json_path.exists():
            return []
        with open(self.json_path, encoding="utf-8") as f:
            return json.load(f)

    def get_processed_ids(self) -> set[str]:
        """Return set of wikidata_ids already saved (for resuming)."""
        return set(self._checkpoint.get("processed_ids", {}).keys())

    def filter_unprocessed(self, cities: list[dict]) -> list[dict]:
        """Return only cities not yet saved (for resuming interrupted runs)."""
        done = self.get_processed_ids()
        remaining = [c for c in cities if c.get("wikidata_id") not in done]
        if len(remaining) < len(cities):
            logger.info(
                "Checkpoint: skipping %d already-processed cities, %d remaining",
                len(cities) - len(remaining), len(remaining)
            )
        return remaining

    # ------------------------------------------------------------------
    # Run logging
    # ------------------------------------------------------------------

    def log_run(self, event: str, metadata: Optional[dict] = None):
        """Append a timestamped event to the run log (JSONL format)."""
        entry = {
            "ts": datetime.now(timezone.utc).isoformat(),
            "event": event,
            **(metadata or {}),
        }
        with open(self.log_path, "a", encoding="utf-8") as f:
            f.write(json.dumps(entry, default=str) + "\n")

    # ------------------------------------------------------------------
    # Stats & reporting
    # ------------------------------------------------------------------

    def stats(self) -> dict:
        """Return a summary of current data state."""
        cities = self.load_cities()
        if not cities:
            return {"total": 0}

        stubs  = sum(1 for c in cities if c.get("needs_manual_review"))
        has_extract = sum(1 for c in cities if c.get("extract"))
        countries = len({c.get("country_code") for c in cities if c.get("country_code")})
        continents = len({c.get("continent") for c in cities if c.get("continent")})

        return {
            "total":          len(cities),
            "with_extract":   has_extract,
            "stubs_flagged":  stubs,
            "countries":      countries,
            "continents":     continents,
            "data_dir":       str(self.data_dir.resolve()),
        }

    def print_stats(self):
        s = self.stats()
        print("\n── City Pipeline Stats ──────────────────")
        for k, v in s.items():
            print(f"  {k:<20} {v}")
        print("─────────────────────────────────────────\n")

    # ------------------------------------------------------------------
    # Internal helpers
    # ------------------------------------------------------------------

    def _write_csv(self, cities: list[dict]):
        with open(self.csv_path, "w", newline="", encoding="utf-8") as f:
            writer = csv.DictWriter(f, fieldnames=self.CITY_FIELDS_CSV, extrasaction="ignore")
            writer.writeheader()
            for city in cities:
                row = dict(city)
                row["extract_length"] = len(city.get("extract", "") or "")
                writer.writerow(row)

    def _load_checkpoint(self) -> dict:
        if not self.checkpoint_path.exists():
            return {"processed_ids": {}, "created_at": datetime.now(timezone.utc).isoformat()}
        with open(self.checkpoint_path, encoding="utf-8") as f:
            return json.load(f)

    def _save_checkpoint(self):
        self._checkpoint["updated_at"] = datetime.now(timezone.utc).isoformat()
        with open(self.checkpoint_path, "w", encoding="utf-8") as f:
            json.dump(self._checkpoint, f, indent=2)