WITH
  flight_delays AS (
    SELECT
      flight_id,
      aircraft_registration,
      scheduled_departure,
      actual_arrival,
      actual_departure,
      TIMESTAMP_DIFF(actual_departure, scheduled_departure, MINUTE) AS dep_delay
    FROM `etl_db.flights`
  ),
  cascade AS (
    SELECT
      f1.flight_id,
      f2.flight_id AS impacted_flight,
      f1.aircraft_registration,
      f1.dep_delay
    FROM flight_delays f1
    JOIN flight_delays f2
      ON
        f1.aircraft_registration = f2.aircraft_registration
        AND f1.actual_arrival < f2.scheduled_departure
        AND f1.dep_delay > 30
  )
SELECT * FROM cascade;
