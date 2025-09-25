import argparse
from datetime import datetime
from Airline.airlines_generator import AirlinesGenerator

class DataGenRunner:
    """
    Orchestrates dataset generation for a specific domain and pushes to Hugging Face
    """

    def __init__(self, domain: str, hf_repo: str, start_date: str, end_date: str):
        self.domain = domain.lower()
        self.hf_repo = hf_repo
        self.start_date = self.parse_date(start_date)
        self.end_date = self.parse_date(end_date)
        self.validate_dates()

    @staticmethod
    def parse_date(date_str: str) -> datetime:
        try:
            return datetime.strptime(date_str, "%Y-%m-%d")
        except ValueError:
            raise ValueError(f"Invalid date format: {date_str}. Use YYYY-MM-DD.")

    def validate_dates(self):
        if self.start_date > self.end_date:
            raise ValueError("Start date must be before or equal to end date.")

    def run(self):
        print(f"Generating data for domain: {self.domain.capitalize()}")
        print(f"Date range: {self.start_date.date()} to {self.end_date.date()}")

        if self.domain == "airline":
            generator = AirlinesGenerator(
                hf_repo=self.hf_repo,
                start_date=self.start_date,
                end_date=self.end_date
            )
            generator.run_all()
        else:
            raise NotImplementedError(f"Domain '{self.domain}' is not implemented yet.")


def parse_args():
    parser = argparse.ArgumentParser(description="Generate domain-specific datasets and push to Hugging Face")
    parser.add_argument("--domain", type=str, default="Airline", choices=["Airline"], help="Domain to generate dataset for")
    parser.add_argument("--hf-repo", type=str, default="ashok-rajendran/data-torture-lab", help="Hugging Face repository to push the dataset")
    parser.add_argument("--start-date", type=str, default="2025-01-01", help="Start date for data generation (YYYY-MM-DD)")
    parser.add_argument("--end-date", type=str, default="2025-12-31", help="End date for data generation (YYYY-MM-DD)")
    # ----------------- NEW: Output folder for timestamped runs -----------------
    parser.add_argument(
        "--output-folder",
        type=str,
        default=None,
        help="Optional output folder (GitHub Actions timestamped folder)"
    )
    return parser.parse_args()
    

if __name__ == "__main__":
    args = parse_args()
    runner = DataGenRunner(
        domain=args.domain,
        hf_repo=args.hf_repo,
        start_date=args.start_date,
        end_date=args.end_date,
        output_folder=args.output_folder
    )
    runner.run()
