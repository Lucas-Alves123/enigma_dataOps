import os
import json
import shutil
import pytest
from datetime import datetime, timedelta

from consumer.receiver import ConsumerReceiver
from bronze.processor import BronzeProcessor
from silver.processor import SilverProcessor
from gold.processor import GoldProcessor

TEST_DATA_DIR = "test_data_temp"

@pytest.fixture(autouse=True)
def setup_and_teardown():
    if os.path.exists(TEST_DATA_DIR):
        shutil.rmtree(TEST_DATA_DIR)
    yield
    if os.path.exists(TEST_DATA_DIR):
        shutil.rmtree(TEST_DATA_DIR)

def test_consumer_validation_and_bronze_storage():
    bronze_proc = BronzeProcessor(TEST_DATA_DIR)
    consumer = ConsumerReceiver(bronze_proc)

    with pytest.raises(ValueError):
        consumer.receive_message({"invalid_key": "some_data"})
    
    with pytest.raises(ValueError):
        consumer.receive_message("not a dict")

    valid_msg = {"part_index": 1, "payload": "Olá"}
    filepath = consumer.receive_message(valid_msg)
    
    assert os.path.exists(filepath)
    
    with open(filepath, "r", encoding="utf-8") as f:
        stored_data = json.load(f)
    
    assert stored_data["part_index"] == 1
    assert stored_data["payload"] == "Olá"
    assert "ingested_at" in stored_data

def test_silver_deduplication_and_sorting():
    bronze_proc = BronzeProcessor(TEST_DATA_DIR)
    silver_proc = SilverProcessor(TEST_DATA_DIR, bronze_proc)

    events = [
        {"part_index": 2, "payload": "mundo", "ingested_at": (datetime.now() - timedelta(seconds=10)).isoformat()},
        {"part_index": 4, "payload": "  !  ", "ingested_at": (datetime.now() - timedelta(seconds=9)).isoformat()},
        {"part_index": 2, "payload": "mundo", "ingested_at": datetime.now().isoformat()},
        {"part_index": 1, "payload": "Olá", "ingested_at": (datetime.now() - timedelta(seconds=8)).isoformat()},
        {"part_index": 3, "payload": ", este é o enigma", "ingested_at": (datetime.now() - timedelta(seconds=20)).isoformat()},
    ]

    for ev in events:
        bronze_proc.save_raw_event(ev)

    cleaned_records, output_file = silver_proc.process_bronze_data()

    assert len(cleaned_records) == 4
    assert os.path.exists(output_file)

    assert cleaned_records[0]["part_index"] == 1
    assert cleaned_records[1]["part_index"] == 2
    assert cleaned_records[2]["part_index"] == 3
    assert cleaned_records[3]["part_index"] == 4

    assert cleaned_records[0]["payload"] == "Olá"
    assert cleaned_records[1]["payload"] == "mundo"
    assert cleaned_records[2]["payload"] == ", este é o enigma"
    assert cleaned_records[3]["payload"] == "!"

def test_gold_aggregation_and_integrity():
    bronze_proc = BronzeProcessor(TEST_DATA_DIR)
    silver_proc = SilverProcessor(TEST_DATA_DIR, bronze_proc)
    gold_proc = GoldProcessor(TEST_DATA_DIR, silver_proc)

    complete_events = [
        {"part_index": 2, "payload": "mundo"},
        {"part_index": 4, "payload": "!"},
        {"part_index": 1, "payload": "Olá"},
        {"part_index": 3, "payload": ", este é o enigma"}
    ]
    for ev in complete_events:
        bronze_proc.save_raw_event(ev)

    silver_proc.process_bronze_data()
    gold_record = gold_proc.process_silver_data()

    assert gold_record["full_payload"] == "Olá mundo, este é o enigma!"
    assert gold_record["data_integrity_status"] == "COMPLETED"
    assert len(gold_record["missing_indices"]) == 0
    
    gold_file = os.path.join(TEST_DATA_DIR, "gold", "gold_final.json")
    assert os.path.exists(gold_file)

    bronze_proc.clear()
    silver_proc.clear()
    gold_proc.clear()

    incomplete_events = [
        {"part_index": 1, "payload": "Olá"},
        {"part_index": 3, "payload": ", este é o enigma"},
        {"part_index": 4, "payload": "!"}
    ]
    for ev in incomplete_events:
        bronze_proc.save_raw_event(ev)

    silver_proc.process_bronze_data()
    gold_record_b = gold_proc.process_silver_data()

    assert gold_record_b["full_payload"] == "Olá, este é o enigma!"
    assert gold_record_b["data_integrity_status"] == "INCOMPLETE_GAPS"
    assert gold_record_b["missing_indices"] == [2]
