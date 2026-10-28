
import argparse
import json
import sys
import time
from pathlib import Path

import pandas as pd

# Allow imports from the project root when running this script directly.
PROJECT_ROOT = Path(__file__).resolve().parent.parent
sys.path.insert(0, str(PROJECT_ROOT))

from src.schemas.production_event import ProductionEvent


DATA_PATH = PROJECT_ROOT / "data" / "raw" / "secom.data"
LABELS_PATH = PROJECT_ROOT / "data" / "raw" / "secom_labels.data"

# Zero-based column indices, matching the convention used in exploration.
SENSOR_INDICES = {
    "sensor_060": 60,
    "sensor_104": 104,
    "sensor_130": 130,
    "sensor_029": 29,
    "sensor_511": 511,
    "sensor_317": 317,
    "sensor_125": 125,
    "sensor_126": 126,
}


def load_labels() -> pd.DataFrame:
    """Read labels while preserving each quoted timestamp as one field."""

    label_values = []
    timestamp_values = []

    with LABELS_PATH.open("r", encoding="utf-8") as file:
        for line_number, line in enumerate(file, start=1):
            line = line.strip()

            if not line:
                continue

            # Example: -1 "19/07/2008 11:55:00"
            label_text, separator, timestamp_text = line.partition(" ")

            if not separator:
                raise ValueError(
                    f"Invalid label format on line {line_number}: {line}"
                )

            timestamp_text = timestamp_text.strip().strip('"')

            try:
                label = int(label_text)
            except ValueError as exc:
                raise ValueError(
                    f"Invalid quality label on line {line_number}: "
                    f"{label_text}"
                ) from exc

            if label not in (-1, 1):
                raise ValueError(
                    f"Unexpected quality label on line {line_number}: "
                    f"{label}"
                )

            label_values.append(label)
            timestamp_values.append(timestamp_text)

    return pd.DataFrame(
        {
            "quality_label": label_values,
            "timestamp": timestamp_values,
        }
    )


def load_events() -> list[ProductionEvent]:
    """Load SECOM data and convert rows into validated production events."""

    if not DATA_PATH.exists():
        raise FileNotFoundError(f"Sensor data not found: {DATA_PATH}")

    if not LABELS_PATH.exists():
        raise FileNotFoundError(f"Labels file not found: {LABELS_PATH}")

    print("Loading SECOM dataset...")

    sensor_data = pd.read_csv(
        DATA_PATH,
        sep=r"\s+",
        header=None,
        na_values=["NaN"],
    )

    labels = load_labels()

    if len(sensor_data) != len(labels):
        raise ValueError(
            f"Row count mismatch: {len(sensor_data)} sensor rows "
            f"but {len(labels)} label rows."
        )

    if sensor_data.shape[1] != 590:
        raise ValueError(
            f"Expected 590 sensor columns, found {sensor_data.shape[1]}."
        )

    timestamps = pd.to_datetime(
        labels["timestamp"],
        format="%d/%m/%Y %H:%M:%S",
        errors="coerce",
    )

    invalid_timestamps = timestamps.isna()

    if invalid_timestamps.any():
        examples = labels.loc[
            invalid_timestamps, "timestamp"
        ].head(5).tolist()

        raise ValueError(
            f"Could not parse {invalid_timestamps.sum()} timestamps. "
            f"Examples: {examples}"
        )

    events = []

    for index, row in sensor_data.iterrows():
        sensor_values = {}

        for sensor_name, column_index in SENSOR_INDICES.items():
            value = row.iloc[column_index]

            # Preserve missing measurements instead of replacing them
            # with zero or imputing them.
            sensor_values[sensor_name] = (
                None if pd.isna(value) else float(value)
            )

        event = ProductionEvent(
            event_id=f"secom-{index:06d}",
            event_timestamp=timestamps.iloc[index].to_pydatetime(),
            unit_id=index,
            quality_label=int(labels.iloc[index]["quality_label"]),
            **sensor_values,
        )

        events.append(event)

    print(f"Sensor rows loaded: {len(sensor_data)}")
    print(f"Labels loaded: {len(labels)}")
    print(f"Validated events: {len(events)}")

    return events


def main() -> None:
    parser = argparse.ArgumentParser(
        description="Replay SECOM production measurements into Kafka."
    )

    parser.add_argument(
        "--dry-run",
        action="store_true",
        help="Print events without connecting to Kafka.",
    )
    parser.add_argument(
        "--limit",
        type=int,
        default=5,
        help="Maximum events to process; use 0 for all events.",
    )
    parser.add_argument(
        "--interval",
        type=float,
        default=1.0,
        help="Seconds between published events.",
    )
    parser.add_argument(
        "--bootstrap-servers",
        default="localhost:9092",
        help="Kafka broker address.",
    )
    parser.add_argument(
        "--topic",
        default="production-events",
        help="Kafka topic name.",
    )

    args = parser.parse_args()

    if args.limit < 0:
        parser.error("--limit must be 0 or greater.")

    if args.interval < 0:
        parser.error("--interval must be 0 or greater.")

    events = load_events()

    if args.limit > 0:
        events = events[:args.limit]

    if args.dry_run:
        print("\nDry run: no messages will be sent to Kafka.\n")

        for event in events:
            if hasattr(event, "model_dump"):
                 payload = event.model_dump(mode="json")
            else:
                payload = json.loads(event.json())

            print(
                json.dumps(
                    payload,
                    indent=2,
                    allow_nan=False,
                )
            )

        print(f"\nDry run completed: {len(events)} events.")
        return

    # Only connect to Kafka when dry-run mode is disabled.
    from kafka import KafkaProducer

    producer = KafkaProducer(
        bootstrap_servers=args.bootstrap_servers,
        value_serializer=lambda value: json.dumps(
            value,
            allow_nan=False,
        ).encode("utf-8"),
        acks="all",
        retries=5,
    )

    sent = 0

    try:
        for event in events:
            if hasattr(event, "model_dump"):
                payload = event.model_dump(mode="json")
            else:
                payload = json.loads(event.json())

            # Wait for broker acknowledgement so failures are visible.
            producer.send(
                args.topic,
                value=payload,
            ).get(timeout=30)

            sent += 1

            print(
                f"Published {event.event_id} | "
                f"timestamp={event.event_timestamp.isoformat()} | "
                f"quality_label={event.quality_label}"
            )

            if args.interval > 0:
                time.sleep(args.interval)

    except KeyboardInterrupt:
        print("\nReplay interrupted by user.")

    finally:
        producer.flush()
        producer.close()

    print(f"Replay finished. Published {sent} events.")


if __name__ == "__main__":
    main()
