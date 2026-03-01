WITH
  journey_legs AS (
    SELECT
      b.journey_id,
      b.passenger_id,
      b.flight_id,
      f.actual_arrival,
      f.scheduled_departure,
      ROW_NUMBER()
        OVER (
          PARTITION BY b.journey_id
          ORDER BY f.scheduled_departure
        ) AS leg_sequence
    FROM `etl_db.bookings` b
    JOIN `etl_db.flights` f
      ON b.flight_id = f.flight_id
  ),
  connections AS (
    SELECT
      j1.journey_id,
      j1.passenger_id,
      j1.flight_id AS first_leg,
      j2.flight_id AS second_leg,
      j1.actual_arrival,
      j2.scheduled_departure,
      TIMESTAMP_DIFF(j2.scheduled_departure, j1.actual_arrival, MINUTE)
        AS connection_minutes
    FROM journey_legs j1
    JOIN journey_legs j2
      ON
        j1.journey_id = j2.journey_id
        AND j2.leg_sequence = j1.leg_sequence + 1
  )
SELECT *
FROM connections
WHERE connection_minutes < 45;
