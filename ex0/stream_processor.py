from abc import ABC, abstractmethod
from typing import Any, List, Dict, Union, Optional


class DataProcessor(ABC):

    @abstractmethod
    def process(self, data: Any) -> str:
        pass

    @abstractmethod
    def validate(self, data: Any) -> bool:
        pass

    def format_output(self, result: str) -> str:
        return f"Output: {result}"


class NumericProcessor(DataProcessor):
    def __init__(self) -> None:
        super().__init__()
        print("\nInitializing Numeric Processor...")

    def validate(self, data: Any) -> bool:
        if not isinstance(data, list) or len(data) == 0:
            return False
        return all(isinstance(x, (int, float)) for x in data)

    def process(self, data: Any) -> str:
        try:
            if not self.validate(data):
                raise ValueError("Invalid numeric data")
            total = sum(data)
            count = len(data)
            avg = total / count
            return f"Processed {count} numeric values, sum={total}, avg={avg}"
        except Exception as exc:
            return f"Error processing numeric data: {exc}"

    def format_output(self, result: str) -> str:
        return super().format_output(result)


class TextProcessor(DataProcessor):
    def __init__(self) -> None:
        super().__init__()
        print("\nInitializing Text Processor...")

    def validate(self, data: Any) -> bool:
        return isinstance(data, str) and len(data.strip()) > 0

    def process(self, data: Any) -> str:
        try:
            if not self.validate(data):
                raise ValueError("Invalid text data")
            char_count = len(data)
            word_count = len(data.split())
            return (f"Processed text: {char_count} "
                    f"characters, {word_count} words")
        except Exception as exc:
            return f"Error processing text data: {exc}"

    def format_output(self, result: str) -> str:
        return super().format_output(result)


def _parse_log(data: str) -> Dict[str, str]:
    level_part, message_part = data.split(":", 1)
    return {
        "level": level_part.strip().upper(),
        "message": message_part.strip(),
    }


class LogProcessor(DataProcessor):
    def __init__(self) -> None:
        super().__init__()
        print("\nInitializing Log Processor...")

    def validate(self, data: Any) -> bool:
        if not isinstance(data, str) or len(data.strip()) == 0:
            return False
        return ":" in data

    def process(self, data: Any) -> str:
        try:
            if not self.validate(data):
                raise ValueError("Invalid log entry")
            parsed = _parse_log(data)
            level = parsed["level"]
            message = parsed["message"]
            if level == "ERROR":
                return f"[ALERT] ERROR level detected: {message}"
            return f"[INFO] {level} level detected: {message}"
        except Exception as exc:
            return f"Error processing log data: {exc}"

    def format_output(self, result: str) -> str:
        return super().format_output(result)


def _format_payload(payload: Optional[Any]) -> str:
    if isinstance(payload, str):
        return f"\"{payload}\""
    return f"{payload}"


def _validation_message(processor: DataProcessor, is_valid: bool) -> str:
    if not is_valid:
        return "Validation: data invalid"
    if isinstance(processor, NumericProcessor):
        return "Validation: Numeric data verified"
    if isinstance(processor, TextProcessor):
        return "Validation: Text data verified"
    if isinstance(processor, LogProcessor):
        return "Validation: Log entry verified"
    return "Validation: data verified"


def _process_payload(processor: DataProcessor, payload: Optional[Any]) -> None:
    print(f"Processing data: {_format_payload(payload)}")
    is_valid = processor.validate(payload)
    print(_validation_message(processor, is_valid))
    result = processor.process(payload)
    print(processor.format_output(result))


def main() -> None:
    print("=== CODE NEXUS - DATA PROCESSOR FOUNDATION ===")

    sample_numeric: List[Union[int, float]] = [1, 2, 3, 4, 5]
    sample_text: str = "Hello Nexus World"
    sample_log: str = "ERROR: Connection timeout"

    processor_classes = [NumericProcessor, TextProcessor, LogProcessor]
    payloads: List[Any] = [sample_numeric, sample_text, sample_log]
    processors: List[DataProcessor] = []

    for processor_class, payload in zip(processor_classes, payloads):
        processor = processor_class()
        processors.append(processor)
        _process_payload(processor, payload)

    print("\n=== Polymorphic Processing Demo ===")

    print("Processing multiple data types through same interface...")
    more_payloads: List[Any] = [
        [1, 2, 3],
        "Hello Nexus",
        "INFO: System ready",
    ]

    for index, (processor, payload) in enumerate(
        zip(processors, more_payloads), start=1
    ):
        result = processor.process(payload)
        print(f"Result {index}: {result}")

    print("\nFoundation systems online. Nexus ready for advanced streams.")


if __name__ == "__main__":
    main()
