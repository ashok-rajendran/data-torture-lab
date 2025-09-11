import csv
import random
from datetime import datetime, timedelta
from faker import Faker
import numpy as np
import os
import zipfile

fake = Faker()
random.seed(42)
np.random.seed(42)

# -------------------------
# CONFIG
# -------------------------
OUTPUT_DIR = "airline_dataset_large"
os.makedirs(OUTPUT_DIR, exist_ok=True)

NUM_AIRPORTS = 100_000               # total airports
NUM_PASSENGERS = 100_000_000         # total unique passengers
NUM_FLIGHTS = 3_000_000              # total unique flights
NUM_BOOKINGS = 1_000_000_000         # total bookings

ROWS_PER_CHUNK = 100_000             # max rows per CSV chunk (all datasets)

PAST_YEARS = 10                     # flights scheduled from 10 years ago
FUTURE_YEARS = 1                    # flights scheduled up to 1 year in future

MAX_STOPS = 2

AIRLINES = [
    "Air India","Emirates","Qatar Airways","United","Delta","Lufthansa","Singapore Airlines",
    "IndiGo","Vistara","SpiceJet","GoAir","Cathay","British Airways","ANA","Air France",
    "KLM","Japan Airlines","Turkish Airlines","Southwest","American Airlines","Alaska Airlines",
    "Jet Airways","Air Canada","Qantas","Etihad Airways","Saudia","Aeroflot","Swiss Air",
    "Air China","China Southern","Copa Airlines","LATAM","Norwegian","Iberia","Finnair",
    "Aer Lingus","Vietnam Airlines","Philippine Airlines","Thai Airways","Korean Air",
    "Asiana Airlines","Royal Jordanian","Gulf Air","Oman Air","Kenya Airways","Ethiopian Airlines",
    "SAS","TAP Portugal","WestJet","Air New Zealand","Alitalia","Malindo Air"
]

AIRCRAFT_TYPES = ["A320","B737-800","ATR72","B787","A321neo","B777-300ER","A350"]

FF_TIERS = ["None","Silver","Gold","Platinum"]
FF_WEIGHTS = [0.72, 0.17, 0.08, 0.03]

SEAT_CLASSES = ["Economy","Premium Economy","Business","First"]
SEAT_CLASS_WEIGHTS = [0.74, 0.12, 0.12, 0.02]

PAYMENT_METHODS = ["Credit Card","Debit Card","UPI","Net Banking","Cash","Miles"]
MEAL_PREFS = ["Veg","Non-Veg","Vegan","Gluten-Free","No Meal","Kosher","Halal"]
WEATHERS = ["Clear","Partly Cloudy","Overcast","Light Rain","Heavy Rain","Thunderstorms","Fog","Windy","Snow","Hail"]
FLIGHT_STATUSES = ["On Time","Delayed","Cancelled","Diverted"]
DELAY_CAUSES = ["Weather","Technical","Crew","ATC","Security","Late Incoming Aircraft","Other"]

# -------------------------
# Helper functions
# -------------------------
def random_airport_code(i):
    return f"AP{str(i).zfill(6)}"  # 6 digits for 100k airports

def iso(dt):
    return dt.isoformat()

def random_seat():
    row = random.randint(1, 55)
    seat = random.choice(list("ABCDEF"))
    return f"{row}{seat}"

# -------------------------
# Chunk counts
# -------------------------
airport_chunks = (NUM_AIRPORTS + ROWS_PER_CHUNK - 1) // ROWS_PER_CHUNK
passenger_chunks = (NUM_PASSENGERS + ROWS_PER_CHUNK - 1) // ROWS_PER_CHUNK
flight_chunks = (NUM_FLIGHTS + ROWS_PER_CHUNK - 1) // ROWS_PER_CHUNK
booking_chunks = (NUM_BOOKINGS + ROWS_PER_CHUNK - 1) // ROWS_PER_CHUNK

START_DATE = datetime.utcnow() - timedelta(days=365 * PAST_YEARS)
END_DATE = datetime.utcnow() + timedelta(days=365 * FUTURE_YEARS)

def random_flight_datetime():
    total_days = (END_DATE - START_DATE).days
    rand_day_offset = random.randint(0, total_days)
    rand_hour = random.randint(0,23)
    rand_minute = random.choice([0,15,30,45])
    return START_DATE + timedelta(days=rand_day_offset, hours=rand_hour, minutes=rand_minute)

# -------------------------
# Generate airports chunked
# -------------------------
print(f"Generating {NUM_AIRPORTS} airports in {airport_chunks} chunk(s)...")
for chunk_idx in range(airport_chunks):
    start_idx = chunk_idx * ROWS_PER_CHUNK + 1
    end_idx = min(start_idx + ROWS_PER_CHUNK - 1, NUM_AIRPORTS)
    fname = os.path.join(OUTPUT_DIR, f"airports_part_{chunk_idx+1}.csv")
    with open(fname, "w", newline="", encoding="utf-8") as f:
        writer = csv.writer(f)
        writer.writerow(["airport_code","airport_name","city","country","timezone","latitude","longitude"])
        for i in range(start_idx, end_idx+1):
            code = random_airport_code(i)
            city = fake.city()
            country = fake.country()
            tz = fake.timezone()
            lat = round(float(fake.latitude()), 6)
            lon = round(float(fake.longitude()), 6)
            writer.writerow([code, f"{city} Intl Airport", city, country, tz, lat, lon])
    print(f"  Wrote {fname}")

# Prepare airport codes list (used in flights)
airport_codes = [random_airport_code(i) for i in range(1, NUM_AIRPORTS+1)]

# -------------------------
# Generate passengers chunked
# -------------------------
print(f"Generating {NUM_PASSENGERS} passengers in {passenger_chunks} chunk(s)...")
passenger_ids = []
for chunk_idx in range(passenger_chunks):
    start_idx = chunk_idx * ROWS_PER_CHUNK + 1
    end_idx = min(start_idx + ROWS_PER_CHUNK - 1, NUM_PASSENGERS)
    fname = os.path.join(OUTPUT_DIR, f"passengers_part_{chunk_idx+1}.csv")
    with open(fname, "w", newline="", encoding="utf-8") as f:
        writer = csv.writer(f)
        writer.writerow(["passenger_id","first_name","last_name","gender","dob","nationality","frequent_flyer_status","ff_points","typical_flight_freq_per_year"])
        for i in range(start_idx, end_idx+1):
            pid = f"P{str(i).zfill(10)}"
            passenger_ids.append(pid)
            gender = random.choice(["M","F","O"])
            first = fake.first_name()
            last = fake.last_name()
            dob = fake.date_of_birth(minimum_age=18, maximum_age=85).isoformat()
            nat = fake.country()
            ff = random.choices(FF_TIERS, weights=FF_WEIGHTS, k=1)[0]
            if ff == "None":
                points = 0
                freq = random.choice([0,1,1,2])
            elif ff == "Silver":
                points = random.randint(5000,20000)
                freq = random.choice([2,3,4,5])
            elif ff == "Gold":
                points = random.randint(20000,60000)
                freq = random.choice([5,10,15])
            else:
                points = random.randint(60000,200000)
                freq = random.choice([10,20,40])
            writer.writerow([pid, first, last, gender, dob, nat, ff, points, freq])
    print(f"  Wrote {fname}")

# -------------------------
# Generate flights chunked
# -------------------------
print(f"Generating {NUM_FLIGHTS} flights in {flight_chunks} chunk(s)...")
flights = []  # Store first chunk only in memory for booking sampling (adjust if needed)
for chunk_idx in range(flight_chunks):
    start_idx = chunk_idx * ROWS_PER_CHUNK + 1
    end_idx = min(start_idx + ROWS_PER_CHUNK - 1, NUM_FLIGHTS)
    fname = os.path.join(OUTPUT_DIR, f"flights_part_{chunk_idx+1}.csv")
    with open(fname, "w", newline="", encoding="utf-8") as f:
        writer = csv.writer(f)
        writer.writerow([
            "flight_id","airline","flight_number","origin_airport","destination_airport",
            "departure_time_utc","arrival_time_utc","aircraft_type","distance_km","stops",
            "layover_airports","flight_type","status","delay_minutes","delay_cause","scheduled_duration_min"
        ])
        for i in range(start_idx, end_idx+1):
            fid = f"F{str(i).zfill(10)}"
            airline = random.choice(AIRLINES)
            flight_number = f"{airline.split()[0][:2].upper()}{random.randint(100,9999)}"
            origin = random.choice(airport_codes)
            dest = random.choice(airport_codes)
            while dest == origin:
                dest = random.choice(airport_codes)
            dep_dt = random_flight_datetime()
            distance_km = random.randint(200, 14000)
            dur_hours = max(1, int(distance_km / 800) + random.randint(0,3))
            arr_dt = dep_dt + timedelta(hours=dur_hours, minutes=random.randint(0,59))
            stops = random.choices([0,1,2], weights=[70,25,5], k=1)[0]
            layovers = ""
            if stops > 0:
                layovers_list = random.sample([c for c in airport_codes if c not in (origin,dest)], stops)
                layovers = ";".join(layovers_list)
            flight_type = "Non-stop" if stops == 0 else ("One-stop" if stops == 1 else "Two-stops")
            status = random.choices(FLIGHT_STATUSES, weights=[78,16,4,2], k=1)[0]
            delay_minutes = ""
            delay_cause = ""
            if status == "Delayed":
                delay_minutes = random.randint(10, 480)
                delay_cause = random.choice(DELAY_CAUSES)
            elif status == "Cancelled":
                delay_cause = random.choice(["Weather","Technical","Crew","Security","Strike"])
            elif status == "Diverted":
                delay_minutes = random.randint(30,720)
                delay_cause = random.choice(["Weather","Technical","Other"])
            scheduled_duration_min = int((arr_dt - dep_dt).total_seconds() / 60)
            writer.writerow([fid, airline, flight_number, origin, dest, iso(dep_dt), iso(arr_dt), random.choice(AIRCRAFT_TYPES), distance_km, stops, layovers, flight_type, status, delay_minutes, delay_cause, scheduled_duration_min])
            # store flights from first chunk for booking sampling only
            if chunk_idx == 0:
                flights.append({
                    "flight_id": fid,
                    "departure_time_utc": iso(dep_dt),
                    "arrival_time_utc": iso(arr_dt),
                    "status": status,
                    "delay_minutes": delay_minutes,
                    "delay_cause": delay_cause,
                    "distance_km": distance_km
                })
    print(f"  Wrote {fname}")

# -------------------------
# Generate bookings chunked
# -------------------------
print(f"Generating {NUM_BOOKINGS} bookings in {booking_chunks} chunk(s)...")
group_counter = 1
group_ids = []

for chunk_idx in range(booking_chunks):
    start_idx = chunk_idx * ROWS_PER_CHUNK + 1
    end_idx = min(start_idx + ROWS_PER_CHUNK - 1, NUM_BOOKINGS)
    fname = os.path.join(OUTPUT_DIR, f"bookings_part_{chunk_idx+1}.csv")
    with open(fname, "w", newline="", encoding="utf-8") as f:
        writer = csv.writer(f)
        writer.writerow([
            "booking_id","flight_id","passenger_id","group_id","booking_date",
            "departure_time_utc","arrival_time_utc","seat_class","ticket_price_usd",
            "payment_method","miles_redeemed","checked_bags","baggage_fee_usd",
            "meal_pref","departure_weather","arrival_weather","flight_status","delay_minutes",
            "delay_cause","show_status","check_in_status","upgraded_at_checkin",
            "seat_number","late_checkin_fee_usd","cancel_reason"
        ])
        for i in range(start_idx, end_idx+1):
            booking_id = f"B{str(i).zfill(15)}"
            fl = random.choice(flights)  # sample only from first flight chunk to save memory
            pid = random.choice(passenger_ids)
            dep_dt = datetime.fromisoformat(fl["departure_time_utc"])
            booking_date = dep_dt - timedelta(days=random.randint(0,365), hours=random.randint(0,23))
            seat_class = random.choices(SEAT_CLASSES, weights=SEAT_CLASS_WEIGHTS, k=1)[0]
            class_multiplier = {"Economy":1.0, "Premium Economy":1.5, "Business":4.0, "First":7.0}
            base_price = max(30, fl["distance_km"] * random.uniform(0.03, 0.25))
            ticket_price = round(base_price * class_multiplier[seat_class] * random.uniform(0.7, 1.4), 2)
            payment_method = random.choices(PAYMENT_METHODS, weights=[45,20,15,10,5,5], k=1)[0]
            checked_bags = 0 if seat_class == "Economy" and random.random() < 0.4 else random.randint(0,3)
            meal_pref = random.choice(MEAL_PREFS)
            dep_weather = random.choice(WEATHERS)
            arr_weather = random.choice(WEATHERS)
            flight_status = fl["status"]
            delay_minutes = fl["delay_minutes"]
            delay_cause = fl["delay_cause"]
            show_status = "Show"
            check_in_status = random.choices(["Not Checked-In","Checked-In","Checked-In (Airport)"], weights=[0.5,0.44,0.06], k=1)[0]
            cancel_reason = ""
            if random.random() < 0.03:
                show_status = "Cancelled"
                check_in_status = "Not Checked-In"
                cancel_reason = random.choice(["Passenger Cancelled","Airline Cancelled","Illness","Visa Issue"])
            elif random.random() < 0.02:
                show_status = "No-Show"
            upgraded = False
            if show_status != "Cancelled" and random.random() < 0.015:
                upgraded = True
                if seat_class == "Economy": seat_class = "Premium Economy"
                elif seat_class == "Premium Economy": seat_class = "Business"
                elif seat_class == "Business": seat_class = "First"
            group_id = ""
            if random.random() < 0.05:
                if random.random() < 0.2:
                    group_id = f"G{group_counter:07d}"
                    group_ids.append(group_id)
                    group_counter += 1
                else:
                    if group_ids and random.random() < 0.5:
                        group_id = random.choice(group_ids)
            miles_redeemed = 0
            if payment_method == "Miles":
                miles_redeemed = int(ticket_price * random.uniform(40,100))
                ticket_price = 0.0
            seat_number = random_seat()
            baggage_fee = 0.0
            if checked_bags > 0 and random.random() < 0.25:
                baggage_fee = round(10 * checked_bags * random.uniform(0.8, 2.5), 2)
            late_checkin_fee = 0.0
            if check_in_status == "Checked-In (Airport)" and random.random() < 0.2:
                late_checkin_fee = round(random.uniform(5, 120), 2)

            writer.writerow([
                booking_id, fl["flight_id"], pid, group_id, iso(booking_date),
                fl["departure_time_utc"], fl["arrival_time_utc"], seat_class, ticket_price,
                payment_method, miles_redeemed, checked_bags, baggage_fee,
                meal_pref, dep_weather, arr_weather, flight_status, delay_minutes,
                delay_cause, show_status, check_in_status, upgraded, seat_number, late_checkin_fee, cancel_reason
            ])
    print(f"  Wrote {fname}")

print("Data generation completed!")
