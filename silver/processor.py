import os
import json
from datetime import datetime
from bronze.processor import BronzeProcessor

class SilverProcessor:
    def __init__(self, base_dir: str = "data", bronze_processor: BronzeProcessor = None):
        self.base_dir = base_dir
        self.bronze_processor = bronze_processor or BronzeProcessor(base_dir)
        self.silver_dir = os.path.join(base_dir, "silver")
        os.makedirs(self.silver_dir, exist_ok=True)

    def process_bronze_data(self) -> tuple[list, str]:
        raw_events = self.bronze_processor.get_all_raw_events()
        if not raw_events:
            print("[Silver] Warning: No raw data found in Bronze Layer to process.")
            return [], ""

        print(f"[Silver] Ingesting {len(raw_events)} events from Bronze.")

        deduplicated = {}
        for event in raw_events:
            part_index = int(event["part_index"])
            ingested_at = event.get("ingested_at", "")
            
            if part_index in deduplicated:
                existing_event = deduplicated[part_index]
                existing_ingested_at = existing_event.get("ingested_at", "")
                if ingested_at > existing_ingested_at:
                    deduplicated[part_index] = event
            else:
                deduplicated[part_index] = event

        sorted_indices = sorted(deduplicated.keys())
        cleaned_records = []

        for idx in sorted_indices:
            raw_record = deduplicated[idx]
            cleaned_payload = str(raw_record.get("payload", "")).strip()
            
            cleaned_record = {
                "part_index": idx,
                "payload": cleaned_payload,
                "cleaned_at": datetime.now().isoformat(),
                "original_ingested_at": raw_record.get("ingested_at")
            }
            cleaned_records.append(cleaned_record)
            print(f"[Silver] Cleaned Part {idx}: {cleaned_payload}")

        filepath = os.path.join(self.silver_dir, "cleaned_events.json")
        with open(filepath, "w", encoding="utf-8") as f:
            json.dump(cleaned_records, f, indent=4, ensure_ascii=False)

        return cleaned_records, filepath

    def clear(self):
        if os.path.exists(self.silver_dir):
            for filename in os.listdir(self.silver_dir):
                filepath = os.path.join(self.silver_dir, filename)
                if os.path.isfile(filepath):
                    os.remove(filepath)
