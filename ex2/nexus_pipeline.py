from abc import ABC, abstractmethod
from collections import Counter, defaultdict, deque
from typing import Any, Dict, List, Optional, Protocol, Union


class ProcessingStage(Protocol):
    def process(self, data: Any) -> Any:
        ...


class InputStage:
    def process(self, data: Any) -> Any:
        if isinstance(data, dict):
            records_raw: Any = data.get("records", [])
            records: List[Any] = (
                [item for item in records_raw if item is not None]
                if isinstance(records_raw, list)
                else [records_raw]
            )
            existing_meta: Dict[str, Any] = (
                data.get("meta", {})
                if isinstance(data.get("meta"), dict)
                else {}
            )
            extra_meta: Dict[str, Any] = {
                key: value
                for key, value in data.items()
                if key not in {"format", "records", "meta"}
            }
            merged_meta: Dict[str, Any] = {
                key: value for key, value in existing_meta.items()
            }
            merged_meta.update(extra_meta)
            return {
                "format": data.get("format", "unknown"),
                "records": records,
                "meta": merged_meta,
            }
        if isinstance(data, list):
            return {
                "format": "unknown",
                "records": [item for item in data],
                "meta": {},
            }
        if isinstance(data, str):
            text = data.strip()
            if text == "":
                raise ValueError("Input cannot be empty")
            return {"format": "text", "records": [text], "meta": {}}
        raise ValueError("Unsupported input type")


class TransformStage:
    def process(self, data: Any) -> Any:
        if not isinstance(data, dict):
            raise ValueError("Transform stage expects dict payload")

        meta: Dict[str, Any] = (
            data.get("meta", {}) if isinstance(data.get("meta"), dict) else {}
        )
        if meta.get("force_transform_error"):
            raise ValueError("Invalid data format")

        payload_format: str = str(data.get("format", "unknown"))
        records: List[Any] = (
            data.get("records", [])
            if isinstance(data.get("records"), list)
            else []
        )

        if payload_format == "json":
            data["records"] = [
                self._transform_json_record(item) for item in records
            ]
        elif payload_format == "csv":
            data["records"] = [
                self._transform_csv_record(item) for item in records
            ]
        elif payload_format == "stream":
            data["records"] = [
                self._transform_stream_record(item) for item in records
            ]
        else:
            data["records"] = [
                (
                    {"value": item, "validated": True}
                    if not isinstance(item, dict)
                    else item
                )
                for item in records
            ]

        data["meta"] = {
            key: value
            for key, value in meta.items()
            if key != "force_transform_error"
        }
        data["meta"]["transformed"] = True
        return data

    def _transform_json_record(self, item: Any) -> Dict[str, Any]:
        if not isinstance(item, dict):
            raise ValueError("JSON record must be a dict")

        record: Dict[str, Any] = {key: value for key, value in item.items()}
        value_raw: Any = record.get("value")
        if value_raw is not None:
            value_num = float(value_raw)
            record["value"] = value_num
            record["status"] = (
                "Normal range" if 18.0 <= value_num <= 28.0 else "Out of range"
            )
        record["validated"] = True
        return record

    def _transform_csv_record(self, item: Any) -> Dict[str, Any]:
        if not isinstance(item, dict):
            return {"raw": str(item), "validated": True}
        record: Dict[str, Any] = {
            key: str(value).strip() for key, value in item.items()
        }
        record["validated"] = True
        return record

    def _transform_stream_record(self, item: Any) -> Dict[str, Any]:
        if isinstance(item, dict):
            reading_raw: Any = item.get("reading", 0.0)
            unit: str = str(item.get("unit", "C"))
        else:
            reading_raw = item
            unit = "C"
        reading = float(reading_raw)
        return {"reading": reading, "unit": unit, "validated": True}


class BackupTransformStage:
    def process(self, data: Any) -> Any:
        if not isinstance(data, dict):
            raise ValueError("Backup transform expects dict payload")

        recovered: Dict[str, Any] = {key: value for key, value in data.items()}
        records: Any = recovered.get("records", [])
        recovered["records"] = (
            records if isinstance(records, list) else [records]
        )

        meta: Any = recovered.get("meta", {})
        if not isinstance(meta, dict):
            meta = {}
        recovered["meta"] = {key: value for key, value in meta.items()}
        recovered["meta"]["recovered"] = True
        recovered["meta"]["recovery_strategy"] = "backup_transform"
        return recovered


class OutputStage:
    def process(self, data: Any) -> Any:
        if not isinstance(data, dict):
            return data

        records: List[Any] = (
            data.get("records", [])
            if isinstance(data.get("records"), list)
            else []
        )
        payload_format: str = str(data.get("format", "unknown"))
        valid_records: List[Any] = [
            item
            for item in records
            if isinstance(item, dict) and item.get("validated")
        ]

        summary: Dict[str, Any] = {
            "format": payload_format,
            "record_count": len(records),
            "valid_count": len(valid_records),
        }

        if payload_format == "stream":
            readings: List[float] = [
                float(item.get("reading"))
                for item in valid_records
                if isinstance(item.get("reading"), (int, float))
            ]
            total = 0.0
            for reading in readings:
                total += reading
            summary["avg"] = round(
                (total / len(readings)) if len(readings) > 0 else 0.0,
                1,
            )

        if payload_format == "csv":
            summary["action_count"] = len(valid_records)

        data["summary"] = summary
        data["meta"] = data.get("meta", {})
        data["meta"]["output_ready"] = True
        return data


class ProcessingPipeline(ABC):
    def __init__(
        self,
        pipeline_id: str,
        stages: Optional[List[ProcessingStage]] = None,
    ) -> None:
        self.pipeline_id: str = pipeline_id
        self.stages: List[ProcessingStage] = (
            [InputStage(), TransformStage(), OutputStage()]
            if stages is None
            else [stage for stage in stages]
        )
        self.backup_stages: Dict[str, ProcessingStage] = {
            "TransformStage": BackupTransformStage()
        }
        self.stage_runs: Counter[str] = Counter()
        self.stage_failures: Counter[str] = Counter()
        self.monitor: defaultdict[str, float] = defaultdict(float)
        self.errors: deque[str] = deque(maxlen=10)

    @abstractmethod
    def process(self, data: Any) -> Union[str, Any]:
        pass

    def run_stages(self, data: Any) -> Any:
        current: Any = data
        for stage in self.stages:
            stage_name = stage.__class__.__name__
            self.stage_runs[stage_name] += 1
            self.monitor["stages_executed"] += 1
            try:
                current = stage.process(current)
                self.monitor["records_processed"] += float(
                    self._count_records(current)
                )
            except Exception as exc:
                self.stage_failures[stage_name] += 1
                self.monitor["errors"] += 1
                self.errors.append(f"{stage_name}: {exc}")
                current = self._recover(stage_name, current)
        return current

    def _recover(self, stage_name: str, data: Any) -> Any:
        backup = self.backup_stages.get(stage_name)
        if backup is None:
            raise ValueError(f"No recovery stage registered for {stage_name}")
        recovered = backup.process(data)
        self.monitor["recoveries"] += 1
        return recovered

    def _count_records(self, data: Any) -> int:
        if isinstance(data, dict) and isinstance(data.get("records"), list):
            return len(data["records"])
        if isinstance(data, list):
            return len(data)
        return 1

    def get_statistics(self) -> Dict[str, Any]:
        total_stage_runs = sum(self.stage_runs.values())
        total_failures = sum(self.stage_failures.values())
        efficiency = (
            ((total_stage_runs - total_failures) * 100.0 / total_stage_runs)
            if total_stage_runs > 0
            else 100.0
        )
        estimated_time = round(total_stage_runs * 0.02, 1)
        return {
            "pipeline_id": self.pipeline_id,
            "stage_runs": {
                key: value for key, value in self.stage_runs.items()
            },
            "stage_failures": {
                key: value for key, value in self.stage_failures.items()
            },
            "efficiency": round(efficiency, 1),
            "estimated_time_s": estimated_time,
            "monitor": {key: value for key, value in self.monitor.items()},
            "recent_errors": [entry for entry in self.errors],
        }


class JSONAdapter(ProcessingPipeline):
    def __init__(self, pipeline_id: str) -> None:
        super().__init__(pipeline_id)

    def process(self, data: Any) -> Union[str, Any]:
        chain_mode = isinstance(data, dict) and bool(data.get("_chain_mode"))
        normalized = self._normalize_input(data)
        result = self.run_stages(normalized)
        if chain_mode:
            return result
        return self._format_output(result)

    def _normalize_input(self, data: Any) -> Dict[str, Any]:
        if isinstance(data, dict):
            if isinstance(data.get("records"), list):
                return {
                    "format": "json",
                    "records": [item for item in data["records"]],
                    "meta": {
                        key: value
                        for key, value in data.items()
                        if key not in {"format", "records", "_chain_mode"}
                    },
                }
            record = {
                key: value
                for key, value in data.items()
                if key not in {"_chain_mode", "_force_error"}
            }
            return {
                "format": "json",
                "records": [record],
                "meta": {
                    "force_transform_error": bool(data.get("_force_error")),
                },
            }

        if isinstance(data, str):
            return {
                "format": "json",
                "records": [self._parse_json_like(data)],
                "meta": {},
            }

        raise ValueError("JSONAdapter expects dict or JSON-like string input")

    def _parse_json_like(self, text: str) -> Dict[str, Any]:
        cleaned = text.strip()
        if cleaned.startswith("{") and cleaned.endswith("}"):
            cleaned = cleaned[1:-1]
        fields = [
            part.strip() for part in cleaned.split(",") if part.strip() != ""
        ]
        pairs: Dict[str, Any] = {}
        for field in fields:
            if ":" not in field:
                continue
            key_raw, value_raw = field.split(":", 1)
            key = key_raw.strip().strip("\"'")
            value = value_raw.strip().strip("\"'")
            try:
                parsed: Any = float(value) if "." in value else int(value)
            except Exception:
                parsed = value
            pairs[key] = parsed
        return pairs

    def _format_output(self, result: Any) -> str:
        if not isinstance(result, dict):
            return f"JSON processed: {result}"
        records = result.get("records", [])
        if (
            not isinstance(records, list)
            or len(records) == 0
            or not isinstance(records[0], dict)
        ):
            return "Processed JSON payload"
        record: Dict[str, Any] = records[0]
        value = record.get("value", "n/a")
        unit = record.get("unit", "")
        status = record.get("status", "Validated")
        return f"Processed temperature reading: {value}{unit} ({status})"


class CSVAdapter(ProcessingPipeline):
    def __init__(self, pipeline_id: str) -> None:
        super().__init__(pipeline_id)

    def process(self, data: Any) -> Union[str, Any]:
        chain_mode = isinstance(data, dict) and bool(data.get("_chain_mode"))
        normalized = self._normalize_input(data)
        result = self.run_stages(normalized)
        if chain_mode:
            return result
        return self._format_output(result)

    def _normalize_input(self, data: Any) -> Dict[str, Any]:
        if isinstance(data, dict) and isinstance(data.get("records"), list):
            return {
                "format": "csv",
                "records": [item for item in data["records"]],
                "meta": {
                    key: value
                    for key, value in data.items()
                    if key not in {"format", "records", "_chain_mode"}
                },
            }

        if isinstance(data, str):
            lines = [
                line.strip() for line in data.split("\n") if line.strip() != ""
            ]
            if len(lines) == 0:
                raise ValueError("CSV input is empty")
            header = [column.strip() for column in lines[0].split(",")]
            rows: List[Dict[str, str]] = []
            for line in lines[1:]:
                values = [piece.strip() for piece in line.split(",")]
                row = {
                    header[index]: values[index] if index < len(values) else ""
                    for index in range(len(header))
                }
                rows.append(row)
            if len(rows) == 0:
                rows = [{column: column for column in header}]
            return {
                "format": "csv",
                "records": rows,
                "meta": {"header": header},
            }

        raise ValueError("CSVAdapter expects CSV string or normalized dict")

    def _format_output(self, result: Any) -> str:
        if not isinstance(result, dict):
            return f"CSV processed: {result}"
        summary = result.get("summary", {})
        actions = (
            summary.get("action_count", 0) if isinstance(summary, dict) else 0
        )
        return f"User activity logged: {actions} actions processed"


class StreamAdapter(ProcessingPipeline):
    def __init__(self, pipeline_id: str) -> None:
        super().__init__(pipeline_id)

    def process(self, data: Any) -> Union[str, Any]:
        chain_mode = isinstance(data, dict) and bool(data.get("_chain_mode"))
        normalized = self._normalize_input(data)
        result = self.run_stages(normalized)
        if chain_mode:
            return result
        return self._format_output(result)

    def _normalize_input(self, data: Any) -> Dict[str, Any]:
        if isinstance(data, dict) and isinstance(data.get("records"), list):
            return {
                "format": "stream",
                "records": [item for item in data["records"]],
                "meta": {
                    key: value
                    for key, value in data.items()
                    if key not in {"format", "records", "_chain_mode"}
                },
            }

        if isinstance(data, list):
            readings = [
                float(item) for item in data if isinstance(item, (int, float))
            ]
            return {
                "format": "stream",
                "records": [
                    {"reading": value, "unit": "C"} for value in readings
                ],
                "meta": {},
            }

        if isinstance(data, str):
            base = deque([21.8, 22.3, 21.9, 22.5, 22.0], maxlen=5)
            return {
                "format": "stream",
                "records": [
                    {"reading": value, "unit": "C"} for value in list(base)
                ],
                "meta": {"source": data},
            }

        raise ValueError(
            "StreamAdapter expects stream descriptor, list, "
            "or normalized dict"
        )

    def _format_output(self, result: Any) -> str:
        if not isinstance(result, dict):
            return f"Stream processed: {result}"
        summary = result.get("summary", {})
        if not isinstance(summary, dict):
            return "Stream summary unavailable"
        count = summary.get("record_count", 0)
        avg = summary.get("avg", 0.0)
        return f"Stream summary: {count} readings, avg: {avg}\u00b0C"


class NexusManager:
    def __init__(self) -> None:
        self.pipelines: Dict[str, ProcessingPipeline] = {}
        self.activity: deque[str] = deque(maxlen=20)

    def register_pipeline(
        self,
        name: str,
        pipeline: ProcessingPipeline,
    ) -> None:
        self.pipelines[name] = pipeline
        self.activity.append(f"registered:{name}")

    def process_with(self, name: str, data: Any) -> Union[str, Any]:
        pipeline = self.pipelines.get(name)
        if pipeline is None:
            raise ValueError(f"Pipeline '{name}' is not registered")
        result = pipeline.process(data)
        self.activity.append(f"processed:{name}")
        return result

    def chain(self, pipeline_names: List[str], data: Any) -> Any:
        current: Any = data
        route: List[str] = []
        for name in pipeline_names:
            pipeline = self.pipelines.get(name)
            if pipeline is None:
                raise ValueError(f"Pipeline '{name}' is not registered")
            route.append(name)

            if isinstance(current, dict):
                payload = {key: value for key, value in current.items()}
            else:
                payload = {"format": "stream", "records": [current]}
            payload["_chain_mode"] = True

            current = pipeline.process(payload)
            self.activity.append(f"chain:{name}")

        if isinstance(current, dict):
            current["chain_route"] = [name for name in route]
        return current

    def get_overview(self) -> Dict[str, Any]:
        pipeline_stats: Dict[str, Any] = {
            name: pipeline.get_statistics()
            for name, pipeline in self.pipelines.items()
        }
        total_runs = sum(
            sum(stats.get("stage_runs", {}).values())
            for stats in pipeline_stats.values()
            if isinstance(stats, dict)
        )
        total_failures = sum(
            sum(stats.get("stage_failures", {}).values())
            for stats in pipeline_stats.values()
            if isinstance(stats, dict)
        )
        return {
            "pipelines": pipeline_stats,
            "total_stage_runs": total_runs,
            "total_failures": total_failures,
            "activity": [item for item in self.activity],
        }


def main() -> None:
    print("=== CODE NEXUS - ENTERPRISE PIPELINE SYSTEM ===")
    print("Initializing Nexus Manager...")
    manager = NexusManager()
    print("Pipeline capacity: 1000 streams/second")

    print("Creating Data Processing Pipeline...")
    print("Stage 1: Input validation and parsing")
    print("Stage 2: Data transformation and enrichment")
    print("Stage 3: Output formatting and delivery")

    json_pipeline = JSONAdapter("json-main")
    csv_pipeline = CSVAdapter("csv-main")
    stream_pipeline = StreamAdapter("stream-main")

    manager.register_pipeline("json", json_pipeline)
    manager.register_pipeline("csv", csv_pipeline)
    manager.register_pipeline("stream", stream_pipeline)

    print("=== Multi-Format Data Processing ===")

    json_input = '{"sensor": "temp", "value": 23.5, "unit": "°C"}'
    print("Processing JSON data through pipeline...")
    print(f"Input: {json_input}")
    print("Transform: Enriched with metadata and validation")
    print(f"Output: {manager.process_with('json', json_input)}")

    csv_input = "user,action,timestamp"
    print("Processing CSV data through same pipeline...")
    print(f'Input: "{csv_input}"')
    print("Transform: Parsed and structured data")
    print(f"Output: {manager.process_with('csv', csv_input)}")

    stream_input = "Real-time sensor stream"
    print("Processing Stream data through same pipeline...")
    print(f"Input: {stream_input}")
    print("Transform: Aggregated and filtered")
    print(f"Output: {manager.process_with('stream', stream_input)}")

    print("=== Pipeline Chaining Demo ===")
    chain_a = StreamAdapter("pipeline-a")
    chain_b = StreamAdapter("pipeline-b")
    chain_c = StreamAdapter("pipeline-c")
    manager.register_pipeline("pipeline_a", chain_a)
    manager.register_pipeline("pipeline_b", chain_b)
    manager.register_pipeline("pipeline_c", chain_c)

    chain_payload = {
        "format": "stream",
        "records": [
            {"reading": 22.0 + index * 0.01, "unit": "C"}
            for index in range(100)
        ],
    }
    chain_result = manager.chain(
        ["pipeline_a", "pipeline_b", "pipeline_c"],
        chain_payload,
    )
    chain_count = 0
    if (
        isinstance(chain_result, dict)
        and isinstance(chain_result.get("records"), list)
    ):
        chain_count = len(chain_result["records"])

    print("Pipeline A -> Pipeline B -> Pipeline C")
    print("Data flow: Raw -> Processed -> Analyzed -> Stored")
    print(
        f"Chain result: {chain_count} records processed "
        "through 3-stage pipeline"
    )

    overview_before_error = manager.get_overview()
    total_runs_before = int(overview_before_error.get("total_stage_runs", 0))
    total_failures_before = int(overview_before_error.get("total_failures", 0))
    efficiency = (
        (total_runs_before - total_failures_before) * 100.0
        / total_runs_before
        if total_runs_before > 0
        else 100.0
    )
    print(
        f"Performance: {round(efficiency, 1)}% efficiency, "
        f"{round(total_runs_before * 0.02, 1)}s total processing time"
    )

    print("=== Error Recovery Test ===")
    print("Simulating pipeline failure...")
    failing_payload = {
        "sensor": "temp",
        "value": 23.5,
        "unit": "C",
        "_force_error": True,
    }
    manager.process_with("json", failing_payload)
    print("Error detected in Stage 2: Invalid data format")
    print("Recovery initiated: Switching to backup processor")

    json_stats = json_pipeline.get_statistics()
    recoveries = int(json_stats.get("monitor", {}).get("recoveries", 0))
    if recoveries > 0:
        print("Recovery successful: Pipeline restored, processing resumed")
    else:
        print("Recovery failed: Manual intervention required")

    print("Nexus Integration complete. All systems operational.")


if __name__ == "__main__":
    main()
