import os
import json
import requests
from datetime import datetime
from silver.processor import SilverProcessor

class GoldProcessor:
    def __init__(self, base_dir: str = "data", silver_processor: SilverProcessor = None, api_url: str = "http://127.0.0.1:8000/api/data"):
        self.base_dir = base_dir
        self.silver_processor = silver_processor or SilverProcessor(base_dir)
        self.gold_dir = os.path.join(base_dir, "gold")
        self.api_url = api_url
        os.makedirs(self.gold_dir, exist_ok=True)

    def process_silver_data(self) -> dict:
        silver_filepath = os.path.join(self.base_dir, "silver", "cleaned_events.json")
        if not os.path.exists(silver_filepath):
            print("[Gold] Warning: Cleaned data file from Silver Layer was not found.")
            return {}

        try:
            with open(silver_filepath, "r", encoding="utf-8") as f:
                cleaned_records = json.load(f)
        except (json.JSONDecodeError, IOError) as e:
            print(f"[Gold] Error reading Silver file: {e}")
            return {}

        if not cleaned_records:
            print("[Gold] Warning: No records found in Silver dataset.")
            return {}

        print(f"[Gold] Starting aggregation. Processing {len(cleaned_records)} records...")

        full_message = ""
        indices_received = []
        for record in cleaned_records:
            payload = record["payload"]
            indices_received.append(record["part_index"])
            
            if not full_message:
                full_message = payload
            elif any(payload.startswith(punc) for punc in [",", ".", "!", "?"]):
                full_message += payload
            else:
                full_message += " " + payload

        expected_indices = list(range(min(indices_received), max(indices_received) + 1))
        missing_indices = [idx for idx in expected_indices if idx not in indices_received]
        
        integrity_ok = len(missing_indices) == 0
        total_parts = len(cleaned_records)

        gold_record = {
            "pipeline_run_id": datetime.now().strftime("run_%Y%m%d_%H%M%S"),
            "full_payload": full_message,
            "assembled_parts_count": total_parts,
            "min_index": min(indices_received),
            "max_index": max(indices_received),
            "missing_indices": missing_indices,
            "data_integrity_status": "COMPLETED" if integrity_ok else "INCOMPLETE_GAPS",
            "processed_at": datetime.now().isoformat(),
        }

        filepath = os.path.join(self.gold_dir, "gold_final.json")
        with open(filepath, "w", encoding="utf-8") as f:
            json.dump(gold_record, f, indent=4, ensure_ascii=False)

        print(f"[Gold] Combined message: '{full_message}'")
        return gold_record

    def send_to_api(self, gold_record: dict) -> bool:
        if not gold_record:
            print("[Gold] Warning: Cannot send empty record to API.")
            return False

        print(f"[Gold] Sending final data to API at {self.api_url}...")
        try:
            response = requests.post(self.api_url, json=gold_record, timeout=3.0)
            if response.status_code in [200, 201]:
                print(f"[Gold] API Integration successful: {response.json()}")
                return True
            else:
                print(f"[Gold] API responded with status code: {response.status_code}")
                return False
        except requests.exceptions.RequestException as e:
            print(f"[Gold] API is offline (Reason: {type(e).__name__}).")
            return False

    def clear(self):
        if os.path.exists(self.gold_dir):
            for filename in os.listdir(self.gold_dir):
                filepath = os.path.join(self.gold_dir, filename)
                if os.path.isfile(filepath):
                    os.remove(filepath)
