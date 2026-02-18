from abc import ABC, abstractmethod
from typing import Any, List, Optional, Dict, Union


class DataStream(ABC):
    def __init__(self, stream_id: str, stream_type: str) -> None:
        self.stream_id: str = stream_id
        self.stream_type: str = stream_type
        self.batches_processed: int = 0
        self.items_processed: int = 0
        self.failed_items: int = 0
        self.last_error: str = ""

    @abstractmethod
    def process_batch(self, data_batch: List[Any]) -> str:
        pass

    def filter_data(
        self, data_batch: List[Any], criteria: Optional[str] = None
    ) -> List[Any]:
        if not isinstance(data_batch, list):
            self.last_error = "Invalid batch type: expected list"
            self.failed_items += 1
            return []

        if criteria is None:
            return [item for item in data_batch]

        criteria_text: str = criteria.lower()
        return [
            item for item in data_batch if criteria_text in str(item).lower()
        ]

    def get_stats(self) -> Dict[str, Union[str, int, float]]:
        return {
            "stream_id": self.stream_id,
            "stream_type": self.stream_type,
            "batches_processed": self.batches_processed,
            "items_processed": self.items_processed,
            "failed_items": self.failed_items,
            "last_error": self.last_error,
        }


def format_batch(data_batch: List[Any]) -> str:
    batch_str = "["
    first = True
    for item in data_batch:
        if not first:
            batch_str += ", "
        batch_str += str(item)
        first = False
    batch_str += "]"
    return batch_str


class SensorStream(DataStream):
    def __init__(self, stream_id: str) -> None:
        super().__init__(stream_id, "Environmental Data")
        self.avg_temp: float = 0.0
        self.critical_alerts: int = 0

    def process_batch(self, data_batch: List[Any]) -> str:

        if not isinstance(data_batch, list):
            self.failed_items += 1
            self.last_error = "Invalid sensor batch: expected list"
            return "Sensor analysis failed: invalid batch"

        self.batches_processed += 1

        total = 0.0
        temp_count = 0
        readings_processed = 0
        critical_count = 0

        for item in data_batch:
            try:
                if not isinstance(item, str):
                    raise ValueError("Sensor item must be a string")

                readings_processed += 1
                if ":" not in item:
                    continue

                key, raw_value = item.split(":", 1)
                key = key.strip().lower()
                value = float(raw_value.strip())

                if key == "temp":
                    total += value
                    temp_count += 1
                    if value >= 30.0:
                        critical_count += 1
                elif key == "humidity" and value >= 80.0:
                    critical_count += 1
            except (ValueError, TypeError) as e:
                self.failed_items += 1
                self.last_error = str(e)

        self.avg_temp = (total / temp_count) if temp_count > 0 else 0.0
        self.critical_alerts += critical_count
        self.items_processed += readings_processed

        return (
            f"Sensor analysis: {readings_processed} readings processed, "
            f"avg temp: {self.avg_temp:.1f}°C"
        )

    def filter_data(
        self, data_batch: List[Any], criteria: Optional[str] = None
    ) -> List[Any]:
        base_filtered = super().filter_data(
            data_batch,
            criteria if criteria != "high-priority" else None,
        )
        if criteria != "high-priority":
            return base_filtered

        filtered: List[Any] = []
        for item in base_filtered:
            if not isinstance(item, str) or ":" not in item:
                continue
            key, raw_value = item.split(":", 1)
            try:
                value = float(raw_value.strip())
            except ValueError:
                continue
            key = key.strip().lower()
            if (
                (key == "temp" and value >= 30.0)
                or (key == "humidity" and value >= 80.0)
            ):
                filtered.append(item)
        return filtered

    def get_stats(self) -> Dict[str, Union[str, int, float]]:
        stats = super().get_stats()
        stats["avg_temp"] = self.avg_temp
        stats["critical_alerts"] = self.critical_alerts
        return stats


class TransactionStream(DataStream):
    def __init__(self, stream_id: str) -> None:
        super().__init__(stream_id, "Financial Data")
        self.net_flow: float = 0.0

    def process_batch(self, data_batch: List[Any]) -> str:
        if not isinstance(data_batch, list):
            self.failed_items += 1
            self.last_error = "Invalid transaction batch: expected list"
            return "Transaction analysis failed: invalid batch"

        self.batches_processed += 1

        ops_processed = 0
        net = 0.0

        for item in data_batch:
            try:
                if not isinstance(item, str) or ":" not in item:
                    raise ValueError("Transaction item must be 'type:value'")
                tx_type, raw_value = item.split(":", 1)
                amount = float(raw_value.strip())
                tx_type = tx_type.strip().lower()
                if tx_type == "buy":
                    net += amount
                elif tx_type == "sell":
                    net -= amount
                else:
                    raise ValueError(f"Unknown transaction type: {tx_type}")
                ops_processed += 1
            except (ValueError, TypeError) as e:
                self.failed_items += 1
                self.last_error = str(e)

        self.net_flow += net
        self.items_processed += ops_processed
        sign = "+" if net >= 0 else ""
        return (
            f"Transaction analysis: {ops_processed} operations, "
            f"net flow: {sign}{net:.0f} units"
        )

    def filter_data(
        self, data_batch: List[Any], criteria: Optional[str] = None
    ) -> List[Any]:
        base_filtered = super().filter_data(
            data_batch,
            criteria if criteria != "high-priority" else None,
        )
        if criteria != "high-priority":
            return base_filtered

        filtered: List[Any] = []
        for item in base_filtered:
            if not isinstance(item, str) or ":" not in item:
                continue
            _, raw_value = item.split(":", 1)
            try:
                amount = float(raw_value.strip())
            except ValueError:
                continue
            if amount >= 120.0:
                filtered.append(item)
        return filtered

    def get_stats(self) -> Dict[str, Union[str, int, float]]:
        stats = super().get_stats()
        stats["net_flow"] = self.net_flow
        return stats


class EventStream(DataStream):
    def __init__(self, stream_id: str) -> None:
        super().__init__(stream_id, "System Events")
        self.error_events: int = 0

    def process_batch(self, data_batch: List[Any]) -> str:
        if not isinstance(data_batch, list):
            self.failed_items += 1
            self.last_error = "Invalid event batch: expected list"
            return "Event analysis failed: invalid batch"

        self.batches_processed += 1

        events_processed = 0
        errors = 0
        for item in data_batch:
            try:
                if not isinstance(item, str):
                    raise ValueError("Event item must be a string")
                events_processed += 1
                if "error" in item.lower():
                    errors += 1
            except ValueError as e:
                self.failed_items += 1
                self.last_error = str(e)

        self.error_events += errors
        self.items_processed += events_processed
        return (
            f"Event analysis: {events_processed} events, "
            f"{errors} error detected" + ("" if errors == 1 else "s")
        )

    def filter_data(
        self, data_batch: List[Any], criteria: Optional[str] = None
    ) -> List[Any]:
        base_filtered = super().filter_data(
            data_batch,
            criteria if criteria != "high-priority" else None,
        )
        if criteria != "high-priority":
            return base_filtered
        return [
            item
            for item in base_filtered
            if isinstance(item, str) and "error" in item.lower()
        ]

    def get_stats(self) -> Dict[str, Union[str, int, float]]:
        stats = super().get_stats()
        stats["error_events"] = self.error_events
        return stats


class StreamProcessor:
    def __init__(self) -> None:
        self.streams: List[DataStream] = []

    def register_stream(self, stream: DataStream) -> None:
        if not isinstance(stream, DataStream):
            raise TypeError("Only DataStream subclasses can be registered")
        self.streams.append(stream)

    def process_all_batches(
        self, stream_batches: Dict[str, List[Any]]
    ) -> List[str]:
        results: List[str] = []
        for stream in self.streams:
            try:
                batch = stream_batches.get(stream.stream_id, [])
                if not isinstance(batch, list):
                    raise TypeError("Batch must be a list")
                stream.process_batch(batch)

                if isinstance(stream, SensorStream):
                    processed_count = len(
                        [item for item in batch if isinstance(item, str)]
                    )
                    label = "Sensor data"
                    noun = "readings"
                elif isinstance(stream, TransactionStream):
                    processed_count = len(
                        [
                            item
                            for item in batch
                            if isinstance(item, str) and ":" in item
                        ]
                    )
                    label = "Transaction data"
                    noun = "operations"
                elif isinstance(stream, EventStream):
                    processed_count = len(
                        [item for item in batch if isinstance(item, str)]
                    )
                    label = "Event data"
                    noun = "events"
                else:
                    processed_count = len(batch)
                    label = f"{stream.stream_type} data"
                    noun = "items"

                summary = f"- {label}: {processed_count} {noun} processed"
                results.append(summary)
            except Exception as e:
                stream.failed_items += 1
                stream.last_error = str(e)
                results.append(f"- {stream.stream_id} failed: {e}")
        return results

    def filter_all(
        self,
        stream_batches: Dict[str, List[Any]],
        criteria: Optional[str],
    ) -> Dict[str, List[Any]]:
        filtered: Dict[str, List[Any]] = {}
        for stream in self.streams:
            try:
                batch = stream_batches.get(stream.stream_id, [])
                filtered[stream.stream_id] = stream.filter_data(
                    batch, criteria
                )
            except Exception as e:
                stream.failed_items += 1
                stream.last_error = str(e)
                filtered[stream.stream_id] = []
        return filtered

    def transform_batch_to_lowercase(self, data_batch: List[Any]) -> List[Any]:
        return [
            item.lower() if isinstance(item, str) else item
            for item in data_batch
        ]


def main():
    print("=== CODE NEXUS - POLYMORPHIC STREAM SYSTEM ===\n")

    print("Initializing Sensor Stream...")
    sensor_data = ["temp:22.5", "humidity:65", "pressure:1013"]
    sensor_stream = SensorStream("SENSOR_001")
    print(
        f"Stream ID: {sensor_stream.stream_id}, "
        f"Type: {sensor_stream.stream_type}"
    )
    print(f"Processing sensor batch: {format_batch(sensor_data)}")
    print(sensor_stream.process_batch(sensor_data))

    print("\nInitializing Transaction Stream...")
    transaction_data = ["buy:100", "sell:150", "buy:75"]
    transaction_stream = TransactionStream("TRANS_001")
    print(
        f"Stream ID: {transaction_stream.stream_id}, "
        f"Type: {transaction_stream.stream_type}"
    )
    print(f"Processing transaction batch: {format_batch(transaction_data)}")
    print(transaction_stream.process_batch(transaction_data))

    print("\nInitializing Event Stream...")
    event_data = ["login", "error", "logout"]
    event_stream = EventStream("EVENT_001")
    print(
        f"Stream ID: {event_stream.stream_id}, "
        f"Type: {event_stream.stream_type}"
    )
    print(f"Processing event batch: {format_batch(event_data)}")
    print(event_stream.process_batch(event_data))

    processor = StreamProcessor()
    processor.register_stream(sensor_stream)
    processor.register_stream(transaction_stream)
    processor.register_stream(event_stream)

    print("\n=== Polymorphic Stream Processing ===")
    print("Processing mixed stream types through unified interface...")

    mixed_batches: Dict[str, List[Any]] = {
        "SENSOR_001": processor.transform_batch_to_lowercase(
            ["TEMP:35.0", "humidity:85"]
        ),
        "TRANS_001": processor.transform_batch_to_lowercase(
            ["buy:40", "sell:120", "sell:35", "buy:10"]
        ),
        "EVENT_001": processor.transform_batch_to_lowercase(
            ["login", "error:disk", "logout"]
        ),
    }

    print("\nBatch 1 Results:")
    for line in processor.process_all_batches(mixed_batches):
        print(line)

    print("\nStream filtering active: High-priority data only")
    filtered = processor.filter_all(mixed_batches, "high-priority")
    sensor_critical = len(filtered.get("SENSOR_001", []))
    large_tx = len(filtered.get("TRANS_001", []))
    print(
        f"Filtered results: {sensor_critical} critical sensor alerts, "
        f"{large_tx} large transaction"
        + ("" if large_tx == 1 else "s")
    )
    print("\nAll streams processed successfully. Nexus throughput optimal.")


if __name__ == "__main__":
    main()
