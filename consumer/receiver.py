from bronze.processor import BronzeProcessor

class ConsumerReceiver:
    def __init__(self, bronze_processor=None):
        self.bronze_processor = bronze_processor or BronzeProcessor()

    def receive_message(self, message: dict) -> str:
        if not isinstance(message, dict):
            raise ValueError("Message must be a dictionary.")

        part_index = message.get("part_index")
        payload = message.get("payload")

        if part_index is None or payload is None:
            raise ValueError("Message is missing required fields 'part_index' or 'payload'.")

        print(f"[Consumer] Ingesting part {part_index}: {payload}")
        filepath = self.bronze_processor.save_raw_event(message)
        return filepath
