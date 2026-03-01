INSERT INTO `etl_db.airports`
(
  airport_code,
  airport_name,
  city,
  country,
  latitude,
  longitude,
  timezone,
  airport_type
)
VALUES
('MAA','Chennai International Airport','Chennai','India',12.9941,80.1709,'Asia/Kolkata','International'),
('DEL','Indira Gandhi International Airport','Delhi','India',28.5562,77.1000,'Asia/Kolkata','International'),
('BOM','Chhatrapati Shivaji Maharaj International Airport','Mumbai','India',19.0896,72.8656,'Asia/Kolkata','International'),
('BLR','Kempegowda International Airport','Bangalore','India',13.1986,77.7066,'Asia/Kolkata','International'),
('HYD','Rajiv Gandhi International Airport','Hyderabad','India',17.2403,78.4294,'Asia/Kolkata','International');

INSERT INTO `etl_db.flights` VALUES

-- Aircraft A1 (delay chain)
('F100','AI101','2026-03-01','MAA','DEL','A1',180,
 TIMESTAMP('2026-03-01 06:00:00'),
 TIMESTAMP('2026-03-01 08:30:00'),
 TIMESTAMP('2026-03-01 07:00:00'),
 TIMESTAMP('2026-03-01 09:30:00'),
 'Delayed'),

('F101','AI102','2026-03-01','DEL','BOM','A1',180,
 TIMESTAMP('2026-03-01 10:00:00'),
 TIMESTAMP('2026-03-01 12:00:00'),
 TIMESTAMP('2026-03-01 11:20:00'),
 TIMESTAMP('2026-03-01 13:20:00'),
 'Delayed'),

-- Aircraft A2 normal
('F200','AI201','2026-03-01','MAA','BLR','A2',150,
 TIMESTAMP('2026-03-01 09:00:00'),
 TIMESTAMP('2026-03-01 10:00:00'),
 TIMESTAMP('2026-03-01 09:00:00'),
 TIMESTAMP('2026-03-01 10:00:00'),
 'On Time'),

-- Cancelled flight
('F300','AI301','2026-03-01','BLR','HYD','A3',120,
 TIMESTAMP('2026-03-01 15:00:00'),
 TIMESTAMP('2026-03-01 16:30:00'),
 NULL,
 NULL,
 'Cancelled'),

-- Long route
('F400','AI401','2026-03-01','DEL','MAA','A4',200,
 TIMESTAMP('2026-03-01 20:00:00'),
 TIMESTAMP('2026-03-01 23:00:00'),
 TIMESTAMP('2026-03-01 20:10:00'),
 TIMESTAMP('2026-03-01 23:05:00'),
 'On Time');

 INSERT INTO `etl_db.passengers` VALUES
('P001','Rahul','Sharma','Male','1990-01-01','Indian','FF001','Gold',90),
('P002','Anita','Rao','Female','1988-02-02','Indian','FF002','Silver',60),
('P003','Vikram','Iyer','Male','1985-03-03','Indian','FF003','Platinum',100),
('P004','Sara','Khan','Female','1995-04-04','Indian','FF004','Silver',50),
('P005','Arjun','Menon','Male','1992-05-05','Indian','FF005','Gold',85),
('P006','Neha','Gupta','Female','1993-06-06','Indian','FF006','Bronze',30),
('P007','Karan','Patel','Male','1989-07-07','Indian','FF007','Silver',55),
('P008','Priya','Nair','Female','1994-08-08','Indian','FF008','Platinum',95),
('P009','Rohan','Das','Male','1991-09-09','Indian','FF009','Gold',88),
('P010','Meera','Joshi','Female','1996-10-10','Indian','FF010','Bronze',25),
('P011','David','Mathew','Male','1987-11-11','Indian','FF011','Silver',60),
('P012','Fatima','Ali','Female','1992-12-12','Indian','FF012','Gold',92),
('P013','Suresh','Kumar','Male','1986-01-15','Indian','FF013','Bronze',35),
('P014','Lavanya','Reddy','Female','1993-03-17','Indian','FF014','Gold',89),
('P015','Imran','Sheikh','Male','1984-04-20','Indian','FF015','Silver',58),
('P016','Divya','Iyer','Female','1997-05-22','Indian','FF016','Bronze',20),
('P017','Nikhil','Varma','Male','1990-06-25','Indian','FF017','Platinum',99),
('P018','Aisha','Mir','Female','1998-07-30','Indian','FF018','Silver',65),
('P019','Ganesh','Bhat','Male','1985-08-12','Indian','FF019','Gold',87),
('P020','Tanya','Kapoor','Female','1994-09-14','Indian','FF020','Bronze',28);


INSERT INTO `etl_db.bookings` VALUES

-- Missed connection journey J1
('B001','J1','F100','P001','2026-02-20','Economy',8000,'Credit Card',15,'Confirmed','Boarded',0,NULL),
('B002','J1','F101','P001','2026-02-20','Economy',6000,'Credit Card',10,'Missed','No Show',0,NULL),
('B003','J1','F200','P001','2026-03-01','Economy',0,'Reaccommodation',10,'Rebooked','Boarded',0,'B002'),

-- Cancelled flight refund
('B004','J2','F300','P002','2026-02-21','Business',15000,'UPI',20,'Cancelled','No Show',15000,NULL),

-- Normal booking
('B005','J3','F200','P003','2026-02-22','Business',12000,'Credit Card',18,'Confirmed','Boarded',0,NULL),

-- Loyalty rebooking case
('B006','J4','F100','P003','2026-02-23','Economy',9000,'Credit Card',12,'Confirmed','Boarded',0,NULL),
('B007','J4','F101','P003','2026-02-23','Economy',7000,'Credit Card',12,'Rebooked','Boarded',0,'B006'),

-- Additional realistic traffic
('B008','J5','F400','P004','2026-02-24','Economy',9500,'UPI',15,'Confirmed','Boarded',0,NULL),
('B009','J6','F200','P005','2026-02-24','Economy',5000,'UPI',10,'Confirmed','Boarded',0,NULL),
('B010','J7','F200','P006','2026-02-24','Economy',5000,'Credit Card',8,'Confirmed','Boarded',0,NULL),
('B011','J8','F400','P007','2026-02-24','Business',18000,'Credit Card',20,'Confirmed','Boarded',0,NULL),
('B012','J9','F100','P008','2026-02-25','Economy',8500,'UPI',15,'Confirmed','Boarded',0,NULL),
('B013','J10','F101','P009','2026-02-25','Economy',7000,'UPI',12,'Confirmed','Boarded',0,NULL),
('B014','J11','F200','P010','2026-02-26','Economy',5000,'UPI',10,'Confirmed','Boarded',0,NULL);
INSERT INTO `etl_db.bookings`
(
  booking_id,
  journey_id,
  flight_id,
  passenger_id,
  booking_date,
  seat_class,
  ticket_price,
  payment_method,
  baggage_weight_kg,
  booking_status,
  boarding_status,
  refund_amount,
  rebooking_reference
)
SELECT
  CONCAT('OB', CAST(num AS STRING)),
  'J_OVER',
  'F200',
  CONCAT('P', LPAD(CAST(MOD(num,20)+1 AS STRING),3,'0')),
  DATE('2026-02-27'),   -- ✅ FIXED
  'Economy',
  5000,
  'UPI',
  10,
  IF(num <= 150, 'Confirmed', 'Denied Boarding'),
  IF(num <= 150, 'Boarded', 'Denied'),
  IF(num > 150, 5000, 0),
  NULL
FROM UNNEST(GENERATE_ARRAY(1,155)) AS num;

INSERT INTO `etl_db.flight_status_history` VALUES
(1,'F100',1,TIMESTAMP('2026-03-01 05:30:00'),'Boarding',0,'Boarding started'),
(2,'F100',2,TIMESTAMP('2026-03-01 06:15:00'),'Delayed',60,'Late incoming aircraft'),

(3,'F101',1,TIMESTAMP('2026-03-01 09:45:00'),'Boarding',0,'Boarding started'),
(4,'F101',2,TIMESTAMP('2026-03-01 10:30:00'),'Delayed',80,'Aircraft arrived late from F100'),

(5,'F300',1,TIMESTAMP('2026-03-01 14:00:00'),'Cancelled',0,'Operational issue');
