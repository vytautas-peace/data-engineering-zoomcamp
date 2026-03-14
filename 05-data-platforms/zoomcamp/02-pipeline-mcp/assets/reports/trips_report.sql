/* @bruin

name: reports.trips_report
type: duckdb.sql

depends:
  - staging.trips

materialization:
  type: table

columns:
  - name: trip_date
    type: date
    description: "Calendar date of the trip pickup."
    primary_key: true
  - name: taxi_type
    type: string
    description: "Taxi type (yellow or green)."
    primary_key: true
  - name: payment_type_name
    type: string
    description: "Human-readable payment type."
    primary_key: true

  - name: trip_count
    type: bigint
    description: "Number of trips for the grouping."
    checks:
      - name: non_negative

  - name: total_passengers
    type: bigint
    description: "Total passengers across trips in the group."
    checks:
      - name: non_negative

  - name: total_distance
    type: double
    description: "Total distance across trips in the group."
    checks:
      - name: non_negative

  - name: total_fare
    type: double
    description: "Sum of fare_amount for the group."
    checks:
      - name: non_negative
  - name: total_tips
    type: double
    description: "Sum of tip_amount for the group."
    checks:
      - name: non_negative
  - name: total_revenue
    type: double
    description: "Sum of total_amount for the group."
    checks:
      - name: non_negative
      
  - name: avg_fare
    type: double
    description: "Average fare for the group."
    checks:
      - name: non_negative
  - name: avg_revenue
    type: double
    description: "Average revenue for the group."
  - name: avg_trip_distance
    type: double
    description: "Average trip distance for the group."
    checks:
      - name: non_negative
  - name: avg_passengers
    type: double
    description: "Average passengers for the group."
    checks:
      - name: non_negative

custom_checks:
  - name: row_count_positive
    description: "The report must have at least one row."
    query: |
      SELECT COUNT(*) > 0 from reports.trips_report;
    value: 1

@bruin */

SELECT
    Cast(pickup_datetime as date) as trip_date,
    taxi_type,
    payment_type_name,

    -- Count metrics
    COUNT(*) as trip_count,
    SUM(passenger_count) as total_passengers,

    -- Distance metrics
    SUM(trip_distance) as total_distance,

    -- Revenue metrics
    SUM(fare_amount) as total_fare,
    SUM(tip_amount) as total_tips,
    SUM(total_amount) as total_revenue,

    -- Average metrics
    AVG(fare_amount) as avg_fare,
    AVG(total_amount) as avg_revenue,
    AVG(trip_distance) as avg_trip_distance,
    AVG(passenger_count) as avg_passengers

FROM
    staging.trips

WHERE
    pickup_datetime >= '{{ start_datetime }}' AND
    pickup_datetime < '{{ end_datetime }}'

GROUP BY
    Cast(pickup_datetime as date),
    taxi_type,
    payment_type_name;

