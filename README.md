# PythonModule05

Small OOP-focused Python module with three exercises that build a data-processing system in stages:

1. `ex0`: base processors and polymorphism
2. `ex1`: specialized streams and batch orchestration
3. `ex2`: multi-stage enterprise-style pipelines with recovery

## Requirements

- Python 3.10+ (validated with `python3`)
- No external dependencies

## Project Structure

```text
.
├── ex0/
│   └── stream_processor.py
├── ex1/
│   └── data_stream.py
└── ex2/
    └── nexus_pipeline.py
```

## Run

From the project root:

```bash
python3 ex0/stream_processor.py
python3 ex1/data_stream.py
python3 ex2/nexus_pipeline.py
```

## What Each Exercise Covers

### ex0 - Data Processor Foundation

File: `ex0/stream_processor.py`

- Defines an abstract `DataProcessor` interface.
- Implements:
  - `NumericProcessor` (sum/average)
  - `TextProcessor` (character/word counts)
  - `LogProcessor` (log-level parsing and alerts)
- Demonstrates polymorphism by processing different payload types through the same interface.

Expected behavior:
- Prints validation + processing output for numeric, text, and log samples.

### ex1 - Polymorphic Stream System

File: `ex1/data_stream.py`

- Defines an abstract `DataStream` with shared counters and filtering.
- Implements:
  - `SensorStream` (temperature/humidity analysis + critical alerts)
  - `TransactionStream` (buy/sell net-flow tracking)
  - `EventStream` (error event counting)
- Adds `StreamProcessor` to register multiple streams and process/filter all batches with one interface.

Expected behavior:
- Processes three stream types, then runs a mixed-batch pass and high-priority filtering.

### ex2 - Enterprise Pipeline System

File: `ex2/nexus_pipeline.py`

- Defines staged processing components:
  - `InputStage`
  - `TransformStage`
  - `OutputStage`
- Builds `ProcessingPipeline` with monitoring, failure counters, and backup recovery (`BackupTransformStage`).
- Implements adapters:
  - `JSONAdapter`
  - `CSVAdapter`
  - `StreamAdapter`
- Uses `NexusManager` for pipeline registration, routing, chaining, and overview stats.

Expected behavior:
- Demonstrates multi-format processing, pipeline chaining, and transform-stage recovery after a forced error.

## Learning Goals

- Abstract base classes and protocols
- Polymorphism and interface-driven design
- Batch processing patterns
- Pipeline staging and error recovery
- Runtime statistics and monitoring
