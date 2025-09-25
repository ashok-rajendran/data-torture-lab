import os
from pathlib import Path
from datetime import datetime
import pandas as pd
from faker import Faker
from huggingface_hub import upload_folder
import random
import string

class SharedUtility:
    """
    Base utility class for dataset generation.
    Provides CSV save, Hugging Face push, ID generation, Faker, and common helpers.
    """

    def __init__(self, domain: str, hf_repo: str, output_root="./output"):
        self.domain = domain
        self.hf_repo = hf_repo
        self.output_root = Path(output_root) / domain
        self.timestamp = datetime.now().strftime("%Y%m%d_%H%M%S_%f")[:-3]  # millisecond
        self.faker = Faker()
        self.ensure_dir(self.output_root)

    def ensure_dir(self, path: Path):
        path.mkdir(parents=True, exist_ok=True)

    # ----------------- CSV Save -----------------
    def save_df(self, df: pd.DataFrame, name: str) -> Path:
        """
        Save a DataFrame as CSV with timestamp in filename
        """
        file_path = self.output_root / f"{name}_{self.timestamp}.csv"
        df.to_csv(file_path, index=False)
        print(f"✅ Saved {name} at {file_path}")
        return file_path

    # ----------------- Hugging Face Push -----------------
    def push_to_hf(self):
        """
        Upload the entire output folder to Hugging Face dataset repo.
        """
        hf_token = os.environ.get("HF_TOKEN")
        if not hf_token:
            raise RuntimeError("HF_TOKEN not found in environment variables!")

        print(f"📤 Uploading {self.output_root} to Hugging Face repo {self.hf_repo}...")

        upload_folder(
            folder_path=str(self.output_root),
            repo_id=self.hf_repo,
            token=hf_token,
            repo_type="dataset",
            path_in_repo=self.output_root.name,  # store in a subfolder with domain name
            ignore_patterns=["*.pyc", "__pycache__"]
        )

        print("✅ Uploaded dataset to Hugging Face successfully!")

    # ----------------- ID Generation -----------------
    def generate_id(self, prefix: str = "", length: int = 6) -> str:
        """
        Generate a random alphanumeric ID with optional prefix
        """
        rand_str = ''.join(random.choices(string.ascii_uppercase + string.digits, k=length))
        return f"{prefix}{rand_str}"

    # ----------------- Date Helpers -----------------
    def random_datetime(self, start: datetime, end: datetime) -> datetime:
        """
        Generate a random datetime between start and end
        """
        delta = end - start
        random_seconds = random.randint(0, int(delta.total_seconds()))
        return start + pd.Timedelta(seconds=random_seconds)

    # ----------------- Random Choice Helpers -----------------
    def random_choice(self, choices: list, weights: list = None):
        return random.choices(choices, weights=weights, k=1)[0]

    def random_status(self, scenario_type: str) -> str:
        """
        Return a realistic status based on scenario type
        """
        status_map = {
            "booking": ["confirmed", "cancelled", "no-show", "rescheduled"],
            "flight": ["on-time", "delayed", "cancelled", "rescheduled"],
            "transaction": ["completed", "failed", "refunded"]
        }
        return self.random_choice(status_map.get(scenario_type, ["unknown"]))

    # ----------------- Safe random pick -----------------
    def pick_random(self, id_list: list):
        if not id_list:
            raise ValueError("ID list is empty!")
        return random.choice(id_list)
