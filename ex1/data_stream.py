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
