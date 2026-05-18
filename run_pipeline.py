import os
import time
from datetime import datetime

from consumer.receiver import ConsumerReceiver
from bronze.processor import BronzeProcessor
from silver.processor import SilverProcessor
from gold.processor import GoldProcessor

def run_medallion_pipeline():
    base_dir = "data"
    
    bronze_proc = BronzeProcessor(base_dir)
    silver_proc = SilverProcessor(base_dir, bronze_proc)
    gold_proc = GoldProcessor(base_dir, silver_proc, api_url="http://127.0.0.1:8000/api/data")

    bronze_proc.clear()
    silver_proc.clear()
    gold_proc.clear()

    print("================================================================================")
    print("                MEDALLION DATA PIPELINE - SIMULATION RUN                        ")
    print("================================================================================")
    print(f"Time: {datetime.now().strftime('%Y-%m-%d %H:%M:%S')}")
    print(f"Workspace directory: {os.path.abspath(base_dir)}")
    print("--------------------------------------------------------------------------------")

    stream_events = [
        {"part_index": 2, "payload": "mundo"},
        {"part_index": 4, "payload": "!"},
        {"part_index": 1, "payload": "Olá"},
        {"part_index": 3, "payload": ", este é o enigma"},
        {"part_index": 2, "payload": "mundo"}
    ]

    print("[Producer] Emitting out-of-order events with duplicate:")
    for idx, event in enumerate(stream_events):
        print(f"  Event {idx + 1}: Index = {event['part_index']} | Payload = '{event['payload']}'")
    print("--------------------------------------------------------------------------------")

    print("[Consumer & Bronze] Ingesting stream...")
    consumer = ConsumerReceiver(bronze_proc)
    
    time.sleep(0.5)
    
    for event in stream_events:
        consumer.receive_message(event)
    
    print("--------------------------------------------------------------------------------")
    print("[Silver] Running cleaning, deduplication and sorting...")
    
    time.sleep(0.5)
    
    cleaned_records, silver_file = silver_proc.process_bronze_data()
    print("--------------------------------------------------------------------------------")

    print("[Gold] Reconstructing final combined message...")
    
    time.sleep(0.5)
    
    gold_record = gold_proc.process_silver_data()
    print("--------------------------------------------------------------------------------")

    print("[API] Submitting Gold results to target API REST...")
    
    time.sleep(0.5)
    
    api_success = gold_proc.send_to_api(gold_record)
    
    print("================================================================================")
    if api_success:
        print("SUCCESS: Medallion flow executed successfully and pushed to API.")
    else:
        print("LOCAL PIPELINE VALIDATION SUCCESSFUL (API server is currently offline).")
        print("To enable real-time API integrations, start the server using:")
        print("  python -m uvicorn api.app:app --reload")
    print("================================================================================")

if __name__ == "__main__":
    run_medallion_pipeline()
