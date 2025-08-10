import csv
import random
import string
from datetime import datetime, timedelta
from faker import Faker
import numpy as np
import os
import zipfile

fake = Faker()
random.seed(42)
np.random.seed(42)

# -------------------------
# CONFIG (tweak these)
# -------------------------
OUTPUT_DIR = "airline_dataset_large"
os.makedirs(OUTPUT_DIR, exist_ok=True)

NUM_AIRPORTS = 300              # number of airports (local + international)
NUM_PASSENGERS = 100_000        # # unique passengers
NUM_FLIGHTS = 20_000            # unique flights (date-specific)
NUM_BOOKINGS = 1_000_000        # total bookings to generate
BOOKING_CHUNKS = 5              # split bookings into this many CSV files
FUTURE_DAYS = 180               # schedule flights within next FUTURE_DAYS days
MAX_STOPS = 2                   # upto 2 stops (0,1,2)
AIRLINES = ["Air India","Emirates","Qatar Airways","United","Delta","Lufthansa","Singapore Airlines","IndiGo","Vistara","SpiceJet","GoAir","Cathay"]
AIRCRAFT_TYPES = ["A320","B737-800","ATR72","B787","A321neo","B777-300ER","A350"]

# probabilities / business rules
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
    # produce readable 3-letter-ish codes unique
    return f"AP{str(i).zfill(3)}"

def iso(dt):
    return dt.isoformat()

def random_seat():
    row = random.randint(1, 55)
    seat = random.choice(list("ABCDEF"))
    return f"{row}{seat}"

# -------------------------
# 1) Airports
# -------------------------
airports_file = os.path.join(OUTPUT_DIR, "airports.csv")
print("Generating airports ->", airports_file)
with open(airports_file, "w", newline="", encoding="utf-8") as f:
    writer = csv.writer(f)
    writer.writerow(["airport_code","airport_name","city","country","timezone","latitude","longitude"])
    for i in range(1, NUM_AIRPORTS+1):
        code = random_airport_code(i)
        city = fake.city()
        country = fake.country()
        tz = fake.timezone()
        lat = round(float(fake.latitude()), 6)
        lon = round(float(fake.longitude()), 6)
        writer.writerow([code, f"{city} Intl Airport", city, country, tz, lat, lon])

airport_codes = [random_airport_code(i) for i in range(1, NUM_AIRPORTS+1)]

# -------------------------
# 2) Passengers
# -------------------------
passengers_file = os.path.join(OUTPUT_DIR, "passengers.csv")
print("Generating passengers ->", passengers_file)
with open(passengers_file, "w", newline="", encoding="utf-8") as f:
    writer = csv.writer(f)
    writer.writerow(["passenger_id","first_name","last_name","gender","dob","nationality","frequent_flyer_status","ff_points","typical_flight_freq_per_year"])
    for i in range(1, NUM_PASSENGERS+1):
        pid = f"P{str(i).zfill(7)}"
        gender = random.choice(["M","F","O"])
        first = fake.first_name()
        last = fake.last_name()
        dob = fake.date_of_birth(minimum_age=18, maximum_age=85).isoformat()
        nat = fake.country()
        ff = random.choices(FF_TIERS, weights=FF_WEIGHTS, k=1)[0]
        if ff == "None":
            points = 0
            freq = random.choice([0,1,1,2])  # rare
        elif ff == "Silver":
            points = random.randint(5000,20000); freq = random.choice([2,3,4,5])
        elif ff == "Gold":
            points = random.randint(20000,60000); freq = random.choice([5,10,15])
        else:
            points = random.randint(60000,200000); freq = random.choice([10,20,40])
        writer.writerow([pid, first, last, gender, dob, nat, ff, points, freq])

# Preload passenger id list for fast sampling
passenger_ids = [f"P{str(i).zfill(7)}" for i in range(1, NUM_PASSENGERS+1)]

# -------------------------
# 3) Flights (date-specific)
# -------------------------
# We'll generate NUM_FLIGHTS unique scheduled flights across FUTURE_DAYS.
flights_file = os.path.join(OUTPUT_DIR, "flights.csv")
print("Generating flights ->", flights_file)
flights = []  # small list kept in memory (NUM_FLIGHTS)
with open(flights_file, "w", newline="", encoding="utf-8") as f:
    writer = csv.writer(f)
    writer.writerow([
        "flight_id","airline","flight_number","origin_airport","destination_airport",
        "departure_time_utc","arrival_time_utc","aircraft_type","distance_km","stops","layover_airports","flight_type","status","delay_minutes","delay_cause","scheduled_duration_min"
    ])
    for i in range(1, NUM_FLIGHTS+1):
        fid = f"F{str(i).zfill(7)}"
        airline = random.choice(AIRLINES)
        flight_number = f"{airline.split()[0][:2].upper()}{random.randint(100,9999)}"
        origin = random.choice(airport_codes)
        dest = random.choice(airport_codes)
        while dest == origin:
            dest = random.choice(airport_codes)
        # schedule a date/time in next FUTURE_DAYS days
        dep_dt = datetime.utcnow() + timedelta(days=random.randint(1, FUTURE_DAYS), hours=random.randint(0,23), minutes=random.choice([0,15,30,45]))
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
        # store minimal flight info for booking references
        flights.append({
            "flight_id": fid,
            "departure_time_utc": iso(dep_dt),
            "arrival_time_utc": iso(arr_dt),
            "status": status,
            "delay_minutes": delay_minutes,
            "delay_cause": delay_cause,
            "distance_km": distance_km
        })

# Preload flight_id list for fast sampling
flight_ids = [f["flight_id"] for f in flights]

# -------------------------
# 4) Flight status history (synthetic events per flight)
# -------------------------
status_hist_file = os.path.join(OUTPUT_DIR, "flight_status_history.csv")
print("Generating flight status history ->", status_hist_file)
with open(status_hist_file, "w", newline="", encoding="utf-8") as f:
    writer = csv.writer(f)
    writer.writerow(["event_id","flight_id","event_time_utc","status","note"])
    event_id = 1
    # create 1-3 events per random subset of flights
    for fl in random.sample(flight_ids, k=max(1000, len(flight_ids)//10)):
        num_events = random.randint(1,3)
        base = datetime.utcnow() + timedelta(days=random.randint(0, FUTURE_DAYS))
        for e in range(num_events):
            ev_time = base + timedelta(hours=random.randint(0,72))
            status = random.choice(["Scheduled","On Time","Delayed","Cancelled","Diverted"])
            note = "" if status == "On Time" else random.choice(["Crew late","Tech issue","Weather at origin","ATC delay","Security check"])
            writer.writerow([f"E{event_id:08d}", fl, iso(ev_time), status, note])
            event_id += 1

# -------------------------
# 5) Bookings (STREAM WRITE) split into parts
# -------------------------
print("Generating bookings (streaming) ... this will take a while for", NUM_BOOKINGS, "rows")
rows_per_chunk = NUM_BOOKINGS // BOOKING_CHUNKS
extra = NUM_BOOKINGS % BOOKING_CHUNKS

# open writers for each chunk file to write sequentially (we will open/close per chunk)
def generate_bookings_chunk(chunk_index, n_rows, start_booking_id):
    fname = os.path.join(OUTPUT_DIR, f"bookings_part_{chunk_index+1}.csv")
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
        group_counter = 1
        group_ids = []
        for i in range(n_rows):
            booking_id = f"B{str(start_booking_id + i).zfill(10)}"
            fl = random.choice(flights)  # flights is small enough to sample from in-memory list
            pid = random.choice(passenger_ids)
            # booking_date: up to 365 days before departure or up to same-day bookings
            dep_dt = datetime.fromisoformat(fl["departure_time_utc"])
            booking_date = dep_dt - timedelta(days=random.randint(0, 365), hours=random.randint(0,23))
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
            # cancellation at booking level (3%)
            if random.random() < 0.03:
                show_status = "Cancelled"
                check_in_status = "Not Checked-In"
                cancel_reason = random.choice(["Passenger Cancelled","Airline Cancelled","Illness","Visa Issue"])
            # no-show (2%) but not if cancelled
            elif random.random() < 0.02:
                show_status = "No-Show"
            # upgrades (1.5%)
            upgraded = False
            if show_status != "Cancelled" and random.random() < 0.015:
                upgraded = True
                if seat_class == "Economy": seat_class = "Premium Economy"
                elif seat_class == "Premium Economy": seat_class = "Business"
                elif seat_class == "Business": seat_class = "First"
            # group bookings (5%)
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
    return fname

# Generate each bookings part sequentially
start_id = 1
chunk_paths = []
for c in range(BOOKING_CHUNKS):
    n = rows_per_chunk + (1 if c < extra else 0)
    print(f"Writing chunk {c+1}/{BOOKING_CHUNKS} with {n} rows ...")
    path = generate_bookings_chunk(c, n, start_id)
    chunk_paths.append(path)
    start_id += n

# -------------------------
# 6) Zip outputs (optional)
# -------------------------
ZIP_OUTPUT = os.path.join(OUTPUT_DIR, "airline_dataset_large.zip")
print("Zipping all CSVs to", ZIP_OUTPUT)
with zipfile.ZipFile(ZIP_OUTPUT, "w", compression=zipfile.ZIP_DEFLATED) as zf:
    for fn in os.listdir(OUTPUT_DIR):
        if fn.endswith(".csv"):
            zf.write(os.path.join(OUTPUT_DIR, fn), arcname=fn)

print("DONE. Files generated in:", OUTPUT_DIR)
print("Booking parts:", chunk_paths)

