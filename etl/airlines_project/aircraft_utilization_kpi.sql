SELECT
  aircraft_registration,
  SUM(
    TIMESTAMP_DIFF(actual_arrival, actual_departure, MINUTE))
    / 60 AS total_flight_hours
FROM `etl_db.flights`
WHERE actual_departure IS NOT NULL
GROUP BY aircraft_registration;
