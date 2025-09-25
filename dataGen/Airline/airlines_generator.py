import pandas as pd
from datetime import datetime, timedelta
from shared_utility import SharedUtility
import random
from faker import Faker

class AirlinesGenerator(SharedUtility):
    """
    Generates realistic airline datasets with relational consistency.
    """

    def __init__(self, hf_repo: str, start_date: datetime, end_date: datetime, output_folder: str = None):
        super().__init__(domain="Airline", hf_repo=hf_repo, output_root=output_folder)
        self.start_date = start_date
        self.end_date = end_date
        self.faker = Faker()

        # ID tracking for relational consistency
        self.airport_ids = []
        self.route_ids = []
        self.flight_ids = []
        self.customer_ids = []
        self.booking_ids = []
        self.passenger_ids = []
        self.membership_ids = []
        self.transaction_ids = []
        self.employee_ids = []

        # Airlines & Sample airports
        self.airlines = ["Delta","American","United","Lufthansa","Emirates","Air France","Singapore","Qatar",
                         "British Airways","ANA","Cathay Pacific","Turkish","KLM","Swiss","Etihad"]

        self.sample_airports = [
            ("JFK", "New York", "USA"), ("LAX", "Los Angeles", "USA"),
            ("ORD", "Chicago", "USA"), ("CDG", "Paris", "France"),
            ("FRA", "Frankfurt", "Germany"), ("DXB", "Dubai", "UAE"),
            ("HND", "Tokyo", "Japan"), ("SIN", "Singapore", "Singapore"),
            ("LHR", "London", "UK"), ("DOH", "Doha", "Qatar"),
            ("SYD", "Sydney", "Australia"), ("HKG", "Hong Kong", "China"),
            ("IST", "Istanbul", "Turkey"), ("AMS", "Amsterdam", "Netherlands"),
            ("AUH", "Abu Dhabi", "UAE")
        ]

    # ----------------- Utility Methods -----------------
    def generate_id(self, prefix: str):
        return f"{prefix}{random.randint(100000, 999999)}"

    def pick_random(self, lst):
        return random.choice(lst)

    def random_datetime(self, start: datetime, end: datetime):
        delta = end - start
        seconds = random.randint(0, int(delta.total_seconds()))
        return start + timedelta(seconds=seconds)

    def random_status(self, type_: str):
        if type_ == "flight":
            return random.choices(["Scheduled", "Delayed", "Cancelled"], weights=[0.7,0.2,0.1])[0]
        elif type_ == "booking":
            return random.choices(["Confirmed", "Cancelled", "No-show"], weights=[0.8,0.15,0.05])[0]
        elif type_ == "transaction":
            return random.choices(["Success","Failed"], weights=[0.9,0.1])[0]
        return "Unknown"

    # ----------------- Dataset Generators -----------------
    def generate_airports(self):
        data = []
        for code, city, country in self.sample_airports:
            airport_id = f"AP-{code}"
            self.airport_ids.append(airport_id)
            data.append({
                "airport_id": airport_id,
                "iata_code": code,
                "city": city,
                "country": country
            })
        return self.save_df(pd.DataFrame(data), "airports")

    def generate_routes(self):
        data = []
        route_counter = 1
        for i in range(len(self.airport_ids)):
            for j in range(len(self.airport_ids)):
                if i != j:
                    route_id = f"RT-{route_counter:04d}"
                    self.route_ids.append(route_id)
                    distance = random.randint(300, 12000)
                    flight_time = round(distance / 800, 2)
                    data.append({
                        "route_id": route_id,
                        "source_airport": self.airport_ids[i],
                        "destination_airport": self.airport_ids[j],
                        "distance_km": distance,
                        "flight_time_hr": flight_time
                    })
                    route_counter += 1
        return self.save_df(pd.DataFrame(data), "routes")

    def generate_flights(self, num_flights=2000):
        data = []
        for _ in range(num_flights):
            flight_id = self.generate_id("FL-")
            self.flight_ids.append(flight_id)
            airline = random.choice(self.airlines)
            route_id = self.pick_random(self.route_ids)
            dep_time = self.random_datetime(self.start_date, self.end_date)
            data.append({
                "flight_id": flight_id,
                "flight_number": f"{airline[:2].upper()}{random.randint(100,9999)}",
                "airline": airline,
                "route_id": route_id,
                "departure_time": dep_time,
                "arrival_time": dep_time + timedelta(hours=random.randint(1,15)),
                "status": self.random_status("flight")
            })
        return self.save_df(pd.DataFrame(data), "flights")

    def generate_customers(self, num_customers=7000):
        data = []
        for _ in range(num_customers):
            customer_id = self.generate_id("CU-")
            self.customer_ids.append(customer_id)
            data.append({
                "customer_id": customer_id,
                "name": self.faker.name(),
                "nationality": self.faker.country(),
                "email": self.faker.email()
            })
        return self.save_df(pd.DataFrame(data), "customers")

    def generate_membership(self):
        data = []
        tiers = ["Silver", "Gold", "Platinum"]
        for customer_id in self.customer_ids:
            if random.random() < 0.5:
                membership_id = self.generate_id("MB-")
                self.membership_ids.append(membership_id)
                data.append({
                    "membership_id": membership_id,
                    "customer_id": customer_id,
                    "tier": random.choice(tiers),
                    "points": random.randint(0,50000)
                })
        return self.save_df(pd.DataFrame(data), "membership")

    def generate_employees(self, num_employees=300):
        roles = ["Pilot","Crew","Ground Staff"]
        data = []
        for _ in range(num_employees):
            emp_id = self.generate_id("EMP-")
            self.employee_ids.append(emp_id)
            data.append({
                "employee_id": emp_id,
                "name": self.faker.name(),
                "role": random.choice(roles),
                "airline": random.choice(self.airlines),
                "country": self.faker.country()
            })
        return self.save_df(pd.DataFrame(data), "employees")

    def generate_bookings(self, num_bookings=10000):
        data = []
        for _ in range(num_bookings):
            booking_id = self.generate_id("BK-")
            self.booking_ids.append(booking_id)
            customer_id = self.pick_random(self.customer_ids)
            flight_id = self.pick_random(self.flight_ids)
            booking_datetime = self.random_datetime(self.start_date, self.end_date)
            booking_status = self.random_status("booking")
            data.append({
                "booking_id": booking_id,
                "customer_id": customer_id,
                "flight_id": flight_id,
                "booking_datetime": booking_datetime,
                "status": booking_status
            })
        return self.save_df(pd.DataFrame(data), "bookings")

    # ----------------- Run All -----------------
    def run_all(self):
        self.generate_airports()
        self.generate_routes()
        self.generate_flights()
        self.generate_customers()
        self.generate_membership()
        self.generate_employees()
        self.generate_bookings()
        self.push_to_hf()
