CREATE OR REPLACE TABLE `etl_db.airports` (
    airport_code        STRING,
    airport_name        STRING,
    city                STRING,
    country             STRING,
    latitude            FLOAT64,
    longitude           FLOAT64,
    timezone            STRING,
    airport_type        STRING
);

CREATE OR REPLACE TABLE `etl_db.flights` (
    flight_id               STRING,
    flight_number           STRING,
    departure_date          DATE,
    origin_airport          STRING,
    destination_airport     STRING,
    aircraft_registration   STRING,
    aircraft_capacity       INT64,
    scheduled_departure     TIMESTAMP,
    scheduled_arrival       TIMESTAMP,
    actual_departure        TIMESTAMP,
    actual_arrival          TIMESTAMP,
    flight_status           STRING
);

CREATE OR REPLACE TABLE `etl_db.passengers` (
    passenger_id            STRING,
    first_name              STRING,
    last_name               STRING,
    gender                  STRING,
    date_of_birth           DATE,
    nationality             STRING,
    frequent_flyer_id       STRING,
    frequent_flyer_tier     STRING,
    loyalty_priority_score  INT64
);


CREATE OR REPLACE TABLE `etl_db.bookings` (
    booking_id              STRING,
    journey_id              STRING,
    flight_id               STRING,
    passenger_id            STRING,
    booking_date            DATE,
    seat_class              STRING,
    ticket_price            NUMERIC,
    payment_method          STRING,
    baggage_weight_kg       NUMERIC,
    booking_status          STRING,      -- Confirmed / Cancelled / Rebooked / Missed / Denied Boarding
    boarding_status         STRING,      -- Boarded / No Show / Denied
    refund_amount           NUMERIC,
    rebooking_reference     STRING       -- new booking_id if rebooked
);


CREATE OR REPLACE TABLE `etl_db.flight_status_history` (
    status_id           INT64,
    flight_id           STRING,
    event_sequence      INT64,
    status_timestamp    TIMESTAMP,
    status              STRING,
    delay_minutes       INT64,
    operational_note    STRING
);
