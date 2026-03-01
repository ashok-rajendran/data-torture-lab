SELECT
  p.frequent_flyer_tier,
  COUNT(*) AS denied_count
FROM `etl_db.bookings` b
JOIN `etl_db.passengers` p
  ON b.passenger_id = p.passenger_id
WHERE b.boarding_status = 'Denied'
GROUP BY p.frequent_flyer_tier
ORDER BY denied_count DESC;
