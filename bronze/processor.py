import os
import json
import uuid
from datetime import datetime

class BronzeProcessor:
    def __init__(self, base_dir: str = "data"):
        self.base_dir = base_dir
        self.bronze_dir = os.path.join(base_dir, "bronze")
        os.makedirs(self.bronze_dir, exist_ok=True)

    def save_raw_event(self, event: dict) -> str:
        if not isinstance(event, dict):
            raise ValueError("Event must be a dictionary")
        if "part_index" not in event or "payload" not in event:
            raise ValueError("Event must contain 'part_index' and 'payload' keys")

        raw_record = event.copy()
        if "ingested_at" not in raw_record:
            raw_record["ingested_at"] = datetime.now().isoformat()
        
        timestamp_str = datetime.now().strftime("%Y%m%d_%H%M%S_%f")
        unique_id = str(uuid.uuid4())[:8]
        filename = f"raw_{timestamp_str}_{unique_id}.json"
        filepath = os.path.join(self.bronze_dir, filename)

        with open(filepath, "w", encoding="utf-8") as f:
            json.dump(raw_record, f, indent=4, ensure_ascii=False)

        return filepath

    def get_all_raw_events(self) -> list:
        events = []
        if not os.path.exists(self.bronze_dir):
            return events

        for filename in os.listdir(self.bronze_dir):
            if filename.endswith(".json") and filename.startswith("raw_"):
                filepath = os.path.join(self.bronze_dir, filename)
                try:
                    with open(filepath, "r", encoding="utf-8") as f:
                        events.append(json.load(f))
                except (json.JSONDecodeError, IOError) as e:
                    print(f"Error reading bronze file {filename}: {e}")
        return events

    def clear(self):
        if os.path.exists(self.bronze_dir):
            for filename in os.listdir(self.bronze_dir):
                filepath = os.path.join(self.bronze_dir, filename)
                if os.path.isfile(filepath):
                    os.remove(filepath)
