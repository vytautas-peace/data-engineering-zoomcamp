"""@bruin

name: ingestion.trips
type: python
image: python:3.11
connection: duckdb-default

materialization:
  type: table
  strategy: append

columns:
  - name: taxi_type
    type: string
    description: "Taxi type (e.g. yellow, green)."
  - name: filename
    type: string
    description: "Filename of the ingested data."
  - name: ingested_at
    type: timestamp
    description: "Timestamp when the data was ingested."

@bruin"""

import io
import json
import os
from datetime import datetime

import pandas as pd
import requests
from dateutil.relativedelta import relativedelta

BASE_URL = "https://d37ci6vzurychx.cloudfront.net/trip-data/"


def _parse_date(s: str) -> datetime:
  """Parse YYYY-MM-DD to datetime at start of day."""
  return datetime.strptime(s.strip()[:10], "%Y-%m-%d")


def _get_taxi_types() -> list[str]:
  """Read taxi_types from BRUIN_VARS; default to ['yellow']."""
  raw = os.environ.get("BRUIN_VARS", "{}")
  try:
    vars_ = json.loads(raw) if raw else {}
    types = vars_.get("taxi_types", ["yellow"])
    return types if isinstance(types, list) else ["yellow"]
  except (json.JSONDecodeError, TypeError):
    return ["yellow"]


def materialize():
  """
  Ingest NYC TLC trip parquet files for the run's date window and taxi types.

  Uses BRUIN_START_DATE / BRUIN_END_DATE and taxi_types from BRUIN_VARS.
  Builds one URL per (taxi_type, year, month), fetches parquet, and concatenates.
  Adds taxi_type, filename and ingested_at; keeps all other columns raw.
  """
  start_date = _parse_date(os.environ["BRUIN_START_DATE"])
  end_date = _parse_date(os.environ["BRUIN_END_DATE"])
  taxi_types = _get_taxi_types()
  ingested_at = datetime.utcnow()

  frames = []
  current = datetime(start_date.year, start_date.month, 1)
  end_month = datetime(end_date.year, end_date.month, 1)

  while current <= end_month:
    year_month = current.strftime("%Y-%m")
    for taxi_type in taxi_types:
      filename = f"{taxi_type}_tripdata_{year_month}.parquet"
      url = f"{BASE_URL}{filename}"
      try:
        resp = requests.get(url, timeout=60)
        resp.raise_for_status()
        df = pd.read_parquet(io.BytesIO(resp.content))
        df["taxi_type"] = taxi_type
        df["filename"] = filename
        df["ingested_at"] = ingested_at

        frames.append(df)
      except (requests.RequestException, OSError) as e:
        # Skip missing or unavailable months (e.g. after Nov 2025)
        print(f"Warning: skipping {url}: {e}")
    current += relativedelta(months=1)

  if not frames:
    # Return minimal schema so downstream still has a table to depend on
    return pd.DataFrame(
      columns=[
        "taxi_type",
        "filename",
        "ingested_at",
      ]
    )

  result = pd.concat(frames, ignore_index=True)
  return result

