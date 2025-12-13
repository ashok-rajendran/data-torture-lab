CREATE TABLE airports (
  port_code STRING NOT NULL,
  city_name STRING,
  PRIMARY KEY(port_code) NOT ENFORCED);

CREATE TABLE flights (
  flight_id STRING,
  start_port STRING,
  end_port STRING,
  start_time DATETIME,
  end_time DATETIME);

DELETE FROM airports WHERE TRUE;
DELETE FROM flights WHERE TRUE;

INSERT INTO airports (port_code, city_name)
VALUES
  ('JFK', 'New York'),
  ('LGA', 'New York'),
  ('EWR', 'New York'),
  ('LAX', 'Los Angeles'),
  ('ORD', 'Chicago'),
  ('SFO', 'San Francisco'),
  ('HND', 'Tokyo'),
  ('NRT', 'Tokyo'),
  ('KIX', 'Osaka');

INSERT INTO flights
  (flight_id, start_port, end_port, start_time, end_time)
VALUES
  ('1', 'JFK', 'HND', '2025-06-15 06:00:00', '2025-06-15 18:00:00'),
  ('2', 'JFK', 'LAX', '2025-06-15 07:00:00', '2025-06-15 10:00:00'),
  ('3', 'LAX', 'NRT', '2025-06-15 10:00:00', '2025-06-15 22:00:00'),
  ('4', 'JFK', 'LAX', '2025-06-15 08:00:00', '2025-06-15 11:00:00'),
  ('5', 'LAX', 'KIX', '2025-06-15 11:30:00', '2025-06-15 22:00:00'),
  ('6', 'LGA', 'ORD', '2025-06-15 09:00:00', '2025-06-15 12:00:00'),
  ('7', 'ORD', 'HND', '2025-06-15 11:30:00', '2025-06-15 23:30:00'),
  ('8', 'EWR', 'SFO', '2025-06-15 09:00:00', '2025-06-15 12:00:00'),
  ('9', 'LAX', 'HND', '2025-06-15 13:00:00', '2025-06-15 23:00:00'),
  ('10', 'KIX', 'NRT', '2025-06-15 08:00:00', '2025-06-15 10:00:00');

SELECT * FROM airports;

SELECT * FROM flights;

WITH
  flight_details AS (
    SELECT
      flights.*,
      departure.city_name AS departure_city,
      landing.city_name AS landing_city
    FROM flights AS flights
    INNER JOIN airports AS departure
      ON departure.port_code = flights.start_port
    INNER JOIN airports AS landing
      ON landing.port_code = flights.end_port
    GROUP BY ALL
  ),
  nonstop_flights AS (
    SELECT
      departure_city,
      CAST(NULL AS string) AS layover_city,
      landing_city,
      CAST(flight_id AS string) flight_id,
      date_diff(end_time, start_time, minute) AS journey_duration
    FROM flight_details
    WHERE departure_city = 'New York' AND landing_city = 'Tokyo'
  ),
  one_stop_flights AS (
    SELECT
      departure.departure_city,
      departure.landing_city AS layover_city,
      landing.landing_city,
      concat(departure.flight_id, ',', landing.flight_id) AS flight_id,
      date_diff(landing.end_time, departure.start_time, minute) AS journey_duration
    FROM flight_details AS departure
    INNER JOIN flight_details AS landing
      ON
        departure.departure_city = 'New York'
        AND landing.landing_city = 'Tokyo'
        AND departure.landing_city = landing.departure_city
        AND departure.end_time <= landing.start_time
  )
SELECT * FROM nonstop_flights
UNION ALL
SELECT * FROM one_stop_flights;
