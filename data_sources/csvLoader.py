"""
CSV city loader.

Replaces GeoNames and Wikidata entirely for Phase 1.
Reads filtered_cities.csv directly — zero API calls, instant, offline.

CSV columns used:
    city_name, lat, lng, country_name, country_code, population, id, continent

Usage:
    loader = CSVCityLoader("filtered_cities.csv")
    cities = loader.load(limit=500)
    cities = loader.load(continent="Europe", limit=200)
    cities = loader.load(country_code="ES")
"""

import pandas as pd
from pathlib import Path
from models.city import CityBase


VALID_CONTINENTS = {
    "Asia", "Europe", "Africa",
    "North America", "South America", "Oceania"
}


class CSVCityLoader:
    """
    Loads CityBase objects from the filtered_cities.csv dump.
    Fast, deterministic, no network required.
    """

    def __init__(self, csv_path: str | Path = "filtered_cities.csv"):
        self.csv_path = Path(csv_path)
        self._df: pd.DataFrame | None = None

    # ------------------------------------------------------------------
    # Public API
    # ------------------------------------------------------------------

    def load(
        self,
        limit: int | None = None,
        continent: str | None = None,       # e.g. "Europe"
        country_code: str | None = None,    # e.g. "ES"
        min_population: int = 0,
        balanced: bool = False,             # spread evenly across continents
        balanced_per_continent: int = 100,  # cities per continent if balanced=True
    ) -> list[CityBase]:
        """
        Load and filter cities from the CSV.

        Args:
            limit:                   Max total cities to return (None = all).
            continent:               Filter to one continent.
            country_code:            Filter to one country (ISO-2).
            min_population:          Drop cities below this population.
            balanced:                Return an even spread across all continents.
            balanced_per_continent:  How many cities per continent (if balanced).

        Returns:
            List of CityBase sorted by population descending.
        """
        df = self._get_df()

        # ── Filters ────────────────────────────────────────────────────
        if min_population > 0:
            df = df[df["population"] >= min_population]

        if continent:
            if continent not in VALID_CONTINENTS:
                raise ValueError(f"Unknown continent '{continent}'. Choose from: {VALID_CONTINENTS}")
            df = df[df["continent"] == continent]

        if country_code:
            df = df[df["country_code"].str.upper() == country_code.upper()]

        # ── Balanced sampling ──────────────────────────────────────────
        if balanced and not continent and not country_code:
            chunks = []
            for cont in VALID_CONTINENTS:
                subset = df[df["continent"] == cont].head(balanced_per_continent)
                chunks.append(subset)
            df = pd.concat(chunks).sort_values("population", ascending=False)

        # ── Limit ──────────────────────────────────────────────────────
        if limit:
            df = df.head(limit)

        return [self._row_to_city(row) for _, row in df.iterrows()]

    def stats(self) -> dict:
        """Print a breakdown of the CSV by continent and country count."""
        df = self._get_df()
        breakdown = df.groupby("continent").agg(
            cities=("city_name", "count"),
            countries=("country_code", "nunique"),
            pop_max=("population", "max"),
            pop_min=("population", "min"),
        ).sort_values("cities", ascending=False)
        return breakdown.to_dict("index")

    # ------------------------------------------------------------------
    # Internals
    # ------------------------------------------------------------------

    def _get_df(self) -> pd.DataFrame:
        if self._df is None:
            self._df = self._load_and_clean()
        return self._df.copy()

    def _load_and_clean(self) -> pd.DataFrame:
        df = pd.read_csv(self.csv_path)

        # Normalise column names (handles slight variations in source CSVs)
        df.columns = df.columns.str.strip().str.lower()

        # Use city_ascii as fallback for name if city_name has unicode issues
        if "city_ascii" in df.columns:
            df["display_name"] = df["city_name"].fillna(df["city_ascii"])
        else:
            df["display_name"] = df["city_name"]

        # Numeric population
        df["population"] = pd.to_numeric(df["population"], errors="coerce").fillna(0).astype(int)

        # Use 'lng' column (this CSV uses lng not lon)
        df = df.rename(columns={"lng": "lon"}) if "lng" in df.columns else df

        # Use 'id' column as our stable integer ID
        df["city_id"] = pd.to_numeric(df["id"], errors="coerce").fillna(0).astype(int)

        # Drop rows missing essential fields
        df = df.dropna(subset=["lat", "lon", "country_code"])

        # Sort by population
        df = df.sort_values("population", ascending=False).reset_index(drop=True)

        return df

    @staticmethod
    def _row_to_city(row: pd.Series) -> CityBase:
        return CityBase(
            geonames_id=int(row["city_id"]),
            name=str(row["display_name"]),
            country=str(row["country_name"]),
            country_code=str(row["country_code"]).upper(),
            lat=float(row["lat"]),
            lon=float(row["lon"]),
            population=int(row["population"]),
            timezone="",   # not in this CSV; OSM/Wikipedia will fill context
        )


# ------------------------------------------------------------------
# Smoke test
# ------------------------------------------------------------------

if __name__ == "__main__":
    loader = CSVCityLoader("filtered_cities.csv")

    print("=== Full dataset stats ===")
    for continent, s in loader.stats().items():
        print(f"  {continent:<16} {s['cities']:>4} cities  |  {s['countries']:>3} countries  |  pop {s['pop_min']:>9,} – {s['pop_max']:>12,}")

    print("\n=== Top 10 cities globally ===")
    for c in loader.load(limit=10):
        print(f"  {c.name:<20} {c.country:<20} {c.population:>12,}  ({c.lat:.2f}, {c.lon:.2f})")

    print("\n=== Top 5 European cities ===")
    for c in loader.load(continent="Europe", limit=5):
        print(f"  {c.name:<20} {c.country:<20} {c.population:>12,}")

    print("\n=== Spanish cities ===")
    for c in loader.load(country_code="ES"):
        print(f"  {c.name:<20} {c.population:>10,}")

    print("\n=== Balanced sample (50 per continent) ===")
    balanced = loader.load(balanced=True, balanced_per_continent=50)
    by_cont: dict = {}
    for c in balanced:
        pass  # just count
    print(f"  Total: {len(balanced)} cities")