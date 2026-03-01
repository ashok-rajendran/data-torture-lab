INSERT INTO `etl_db.airports` VALUES
('MAA','Chennai International Airport','Chennai','India',12.9900,80.1693,'Asia/Kolkata','International'),
('BLR','Kempegowda International Airport','Bengaluru','India',13.1986,77.7066,'Asia/Kolkata','International'),
('DEL','Indira Gandhi International Airport','Delhi','India',28.5562,77.1000,'Asia/Kolkata','International'),
('BOM','Chhatrapati Shivaji Maharaj International Airport','Mumbai','India',19.0896,72.8656,'Asia/Kolkata','International'),
('DXB','Dubai International Airport','Dubai','UAE',25.2532,55.3657,'Asia/Dubai','International'),
('CJB','Coimbatore International Airport','Coimbatore','India',11.0300,77.0434,'Asia/Kolkata','Domestic');

INSERT INTO `etl_db.flights` VALUES
('F001','AI101','2026-03-01','MAA','DXB','Boeing 737',TIMESTAMP('2026-03-01 05:30:00'),TIMESTAMP('2026-03-01 08:00:00'),0,'Scheduled'),
('F002','AI102','2026-03-01','DXB','MAA','Boeing 737',TIMESTAMP('2026-03-01 10:00:00'),TIMESTAMP('2026-03-01 14:30:00'),0,'Scheduled'),
('F003','6E201','2026-03-01','MAA','BLR','Airbus A320',TIMESTAMP('2026-03-01 06:00:00'),TIMESTAMP('2026-03-01 07:00:00'),0,'Scheduled'),
('F004','6E202','2026-03-01','BLR','DEL','Airbus A320',TIMESTAMP('2026-03-01 08:30:00'),TIMESTAMP('2026-03-01 11:00:00'),0,'Scheduled'),
('F005','AI305','2026-03-02','DEL','BOM','Boeing 787',TIMESTAMP('2026-03-02 09:00:00'),TIMESTAMP('2026-03-02 11:00:00'),0,'Scheduled'),
('F006','AI306','2026-03-02','BOM','DEL','Boeing 787',TIMESTAMP('2026-03-02 13:00:00'),TIMESTAMP('2026-03-02 15:00:00'),0,'Scheduled'),
('F007','6E450','2026-03-02','CJB','MAA','Airbus A320',TIMESTAMP('2026-03-02 07:30:00'),TIMESTAMP('2026-03-02 08:30:00'),0,'Scheduled'),
('F008','6E451','2026-03-02','MAA','CJB','Airbus A320',TIMESTAMP('2026-03-02 18:00:00'),TIMESTAMP('2026-03-02 19:00:00'),0,'Scheduled'),
('F009','AI777','2026-03-03','MAA','DEL','Boeing 737',TIMESTAMP('2026-03-03 09:00:00'),TIMESTAMP('2026-03-03 12:00:00'),0,'Scheduled'),
('F010','AI778','2026-03-03','DEL','MAA','Boeing 737',TIMESTAMP('2026-03-03 14:00:00'),TIMESTAMP('2026-03-03 17:00:00'),0,'Scheduled');

INSERT INTO `etl_db.passengers`
SELECT
  CONCAT('P', LPAD(CAST(num AS STRING),3,'0')) AS passenger_id,
  CONCAT('First', num) AS first_name,
  CONCAT('Last', num) AS last_name,
  IF(MOD(num,2)=0,'Male','Female') AS gender,
  DATE_SUB(CURRENT_DATE(), INTERVAL (20 + MOD(num,25)) YEAR) AS date_of_birth,
  'Indian' AS nationality,
  CONCAT('FF', num) AS frequent_flyer_id,
  CASE
    WHEN MOD(num,10)=0 THEN 'Platinum'
    WHEN MOD(num,5)=0 THEN 'Gold'
    ELSE 'Silver'
  END AS frequent_flyer_tier
FROM UNNEST(GENERATE_ARRAY(1,100)) AS num;


INSERT INTO `etl_db.bookings`
SELECT
  CONCAT('B', LPAD(CAST(num AS STRING),3,'0')) AS booking_id,
  CONCAT('F00', CAST((MOD(num,10)+1) AS STRING)) AS flight_id,
  CONCAT('P', LPAD(CAST(num AS STRING),3,'0')) AS passenger_id,
  DATE('2026-02-25') AS booking_date,
  CASE
    WHEN MOD(num,3)=0 THEN 'Business'
    WHEN MOD(num,5)=0 THEN 'First'
    ELSE 'Economy'
  END AS seat_class,
  CAST(3000 + MOD(num,5000) AS NUMERIC) AS ticket_price,
  CASE
    WHEN MOD(num,2)=0 THEN 'Credit Card'
    ELSE 'UPI'
  END AS payment_method,
  CAST(MOD(num,25) AS NUMERIC) AS baggage_weight_kg,
  'Confirmed' AS booking_status
FROM UNNEST(GENERATE_ARRAY(1,100)) AS num;


INSERT INTO `etl_db.flight_status_history`
SELECT
  num AS status_id,
  CONCAT('F00', CAST((MOD(num,10)+1) AS STRING)) AS flight_id,
  TIMESTAMP('2026-03-01 04:00:00') + INTERVAL num MINUTE,
  CASE
    WHEN MOD(num,7)=0 THEN 'Delayed'
    WHEN MOD(num,11)=0 THEN 'Cancelled'
    ELSE 'On Time'
  END AS status,
  IF(MOD(num,7)=0, 30, 0) AS delay_minutes,
  'Operational update' AS operational_note
FROM UNNEST(GENERATE_ARRAY(1,30)) AS num;

