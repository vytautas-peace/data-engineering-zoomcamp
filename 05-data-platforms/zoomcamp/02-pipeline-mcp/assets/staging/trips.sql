/* @bruin

# Staging asset: clean, deduplicate, and enrich NYC taxi trips.

name: staging.trips
type: duckdb.sql

depends:
  - ingestion.trips
  - ingestion.payment_lookup

materialization:
  type: table

columns:
  - name: taxi_type
    type: string
    description: "Taxi type (yellow or green)."
  - name: pickup_datetime
    type: timestamp
    description: "Trip pickup timestamp."
    primary_key: true
    nullable: false
    checks:
      - name: not_null
  - name: dropoff_datetime
    type: timestamp
    description: "Trip dropoff timestamp."
    primary_key: true
    nullable: false
    checks:
      - name: not_null
  - name: pickup_location_id
    type: integer
    description: "Pickup zone location ID."
    primary_key: true
    nullable: true
  - name: dropoff_location_id
    type: integer
    description: "Dropoff zone location ID."
    primary_key: true
    nullable: true
  - name: passenger_count
    type: integer
    description: "Number of passengers."
    checks:
      - name: non_negative
  - name: trip_distance
    type: float
    description: "Reported trip distance."
    checks:
      - name: non_negative
  - name: total_amount
    type: float
    description: "Total amount charged to the rider."
    checks:
      - name: non_negative
  - name: payment_type
    type: integer
    description: "Raw payment type code from TLC data."
  - name: payment_type_name
    type: string
    description: "Human-friendly payment type description from lookup."

custom_checks:
  - name: non_negative_total_amount
    description: "All trips in staging must have a non-negative total_amount."
    query: |
      SELECT COUNT(*) = 0
      FROM staging.trips
      WHERE total_amount < 0;
    value: 1

@bruin */

-- Staging layer for NYC taxi trips.
-- Responsibilities:
-- - Use the unified pickup_datetime/dropoff_datetime produced by ingestion.trips
-- - Join payment type IDs to human-readable names
-- - Deduplicate potential duplicate rows using a composite business key
-- - Filter to the time window defined by {{ start_datetime }} / {{ end_datetime }}

WITH base AS (
    SELECT
        t.taxi_type,
        Coalesce(t.tpep_pickup_datetime, t.lpep_pickup_datetime) as pickup_datetime,
        Coalesce(t.tpep_dropoff_datetime, t.lpep_dropoff_datetime) as dropoff_datetime,
        t.pu_location_id AS pickup_location_id,
        t.do_location_id AS dropoff_location_id,
        t.passenger_count,
        t.trip_distance,
        t.total_amount,
        t.fare_amount,
        t.extra,
        t.mta_tax,
        t.tip_amount,
        t.tolls_amount,
        t.improvement_surcharge,
        t.payment_type,
        p.payment_type_name,
        t.filename,
        t.ingested_at,
        ROW_NUMBER() OVER (
            PARTITION BY
                t.taxi_type,
                Coalesce(t.tpep_pickup_datetime, t.lpep_pickup_datetime),
                Coalesce(t.tpep_dropoff_datetime, t.lpep_dropoff_datetime),
                t.pu_location_id,
                t.do_location_id,
                t.total_amount
            ORDER BY
                t.filename
        ) AS rn
    FROM ingestion.trips t
    LEFT JOIN ingestion.payment_lookup p
      ON t.payment_type = p.payment_type_id
    WHERE
      Coalesce(t.tpep_pickup_datetime, t.lpep_pickup_datetime) IS NOT NULL
      AND t.total_amount >= 0
      AND t.fare_amount >= 0
)

SELECT
    pickup_datetime,
    dropoff_datetime,
    pickup_location_id,
    dropoff_location_id,
    taxi_type,
    passenger_count,
    trip_distance,
    payment_type,
    COALESCE(payment_type_name, 'Unknown') as payment_type_name,
    fare_amount,
    extra,
    mta_tax,
    tip_amount,
    tolls_amount,
    improvement_surcharge,
    total_amount,
    ingested_at,
    filename
FROM
    base b
WHERE
    b.rn = 1
    AND b.pickup_datetime >= '{{ start_datetime }}'
    AND b.pickup_datetime < '{{ end_datetime }}';

