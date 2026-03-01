WITH
  flight_revenue AS (
    SELECT
      flight_id,
      SUM(ticket_price) AS total_ticket_revenue,
      SUM(refund_amount) AS total_refund,
      COUNT(*) AS total_bookings
    FROM `etl_db.bookings`
    GROUP BY flight_id
  )
SELECT
  f.flight_id,
  f.aircraft_capacity,
  fr.total_bookings,
  fr.total_ticket_revenue,
  fr.total_refund,
  fr.total_ticket_revenue - fr.total_refund AS net_revenue
FROM `etl_db.flights` f
JOIN flight_revenue fr
  ON f.flight_id = fr.flight_id
WHERE fr.total_bookings > f.aircraft_capacity;
