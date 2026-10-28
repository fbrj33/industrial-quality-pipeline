
from pathlib import Path

import numpy as np
import pandas as pd
from scipy.stats import ttest_ind


ROOT = Path(__file__).resolve().parents[1]
RAW_DIR = ROOT / "data" / "raw"
REPORT_DIR = ROOT / "data" / "processed"

MAX_MISSING_RATE = 0.20
EXPECTED_ROWS = 1567
EXPECTED_SENSORS = 590


def load_dataset():
    """Load and validate the SECOM measurements, labels and timestamps."""
    measurements_path = RAW_DIR / "secom.data"
    labels_path = RAW_DIR / "secom_labels.data"

    if not measurements_path.exists() or not labels_path.exists():
        raise FileNotFoundError(
            "SECOM files are missing. Download them into data/raw/ first."
        )

    # Load sensor measurements.
    X = pd.read_csv(
        measurements_path,
        sep=r"\s+",
        header=None,
        na_values=["NaN"],
        engine="python",
    )

    # In SECOM, each label row contains:
    # label, timestamp (date and time separated by whitespace).
    metadata = pd.read_csv(
        labels_path,
        sep=r"\s+",
        header=None,
        names=["label", "date", "time"],
        engine="python",
    )

    if X.shape[0] != metadata.shape[0]:
        raise ValueError(
            f"Row count mismatch: {X.shape[0]} measurements "
            f"versus {metadata.shape[0]} label records. "
            "Check that both files downloaded completely."
        )

    if X.shape[0] != EXPECTED_ROWS:
        raise ValueError(
            f"Expected {EXPECTED_ROWS} production records, "
            f"but found {X.shape[0]}."
        )

    if X.shape[1] != EXPECTED_SENSORS:
        raise ValueError(
            f"Expected {EXPECTED_SENSORS} sensors, "
            f"but found {X.shape[1]}."
        )

    # Assign stable, human-readable column names.
    X.columns = [
        f"sensor_{i:03d}"
        for i in range(1, X.shape[1] + 1)
    ]

    # Combine the date and time into one timestamp.
    metadata["timestamp"] = pd.to_datetime(
        metadata["date"].astype(str)
        + " "
        + metadata["time"].astype(str),
        format="%d/%m/%Y %H:%M:%S",
        errors="coerce",
    )

    metadata["label"] = pd.to_numeric(
        metadata["label"],
        errors="raise",
    )

    # Validate labels before analysis.
    if not metadata["label"].isin([-1, 1]).all():
        unexpected = sorted(
            metadata.loc[
                ~metadata["label"].isin([-1, 1]), "label"
            ].unique().tolist()
        )
        raise ValueError(f"Unexpected label values: {unexpected}")

    # Preserve row order so measurements remain aligned with labels.
    df = pd.concat(
        [
            metadata[["label", "timestamp"]].reset_index(drop=True),
            X.reset_index(drop=True),
        ],
        axis=1,
    )

    return df, X


def rank_sensors(X, labels):
    """Rank usable sensors by their ability to distinguish outcomes."""
    missing_rate = X.isna().mean()
    passing = labels.eq(-1)
    failing = labels.eq(1)

    candidates = []

    for col in X.columns:
        series = X[col]
        observed = series.dropna()

        # Exclude sensors with excessive missingness or no variation.
        if missing_rate[col] > MAX_MISSING_RATE:
            continue

        if observed.nunique() < 2:
            continue

        # Impute with the overall median for ranking only.
        # These imputed values will NOT be used for SPC limits.
        filled = series.fillna(observed.median())

        pass_values = filled.loc[passing].to_numpy(dtype=float)
        fail_values = filled.loc[failing].to_numpy(dtype=float)

        if len(pass_values) < 2 or len(fail_values) < 2:
            continue

        pass_std = np.std(pass_values, ddof=1)
        fail_std = np.std(fail_values, ddof=1)

        pooled_sd = np.sqrt(
            (pass_std**2 + fail_std**2) / 2
        )

        mean_difference = abs(
            np.mean(pass_values) - np.mean(fail_values)
        )

        if not np.isfinite(pooled_sd) or pooled_sd == 0:
            effect_size = 0.0
        else:
            effect_size = mean_difference / pooled_sd

        test = ttest_ind(
            pass_values,
            fail_values,
            equal_var=False,
        )

        candidates.append(
            {
                "sensor": col,
                "missing_rate": float(missing_rate[col]),
                "observed_std": float(observed.std()),
                "absolute_effect_size": float(effect_size),
                "welch_p_value": float(test.pvalue),
            }
        )

    ranking = pd.DataFrame(candidates)

    if len(ranking) < 8:
        raise ValueError(
            f"Only {len(ranking)} sensors passed the filters. "
            "Review the missing-value threshold and raw data."
        )

    ranking = ranking.sort_values(
        ["absolute_effect_size", "welch_p_value"],
        ascending=[False, True],
    ).reset_index(drop=True)

    ranking.insert(0, "rank", np.arange(1, len(ranking) + 1))

    return ranking


def main():
    REPORT_DIR.mkdir(parents=True, exist_ok=True)

    df, X = load_dataset()

    sensor_cols = X.columns.tolist()
    missing_rate = X.isna().mean()
    label_counts = df["label"].value_counts()

    print("\n=== DATASET OVERVIEW ===")
    print(f"Production records: {len(df)}")
    print(f"Sensor columns: {len(sensor_cols)}")
    print(f"Passing units (-1): {label_counts.get(-1, 0)}")
    print(f"Failing units (+1): {label_counts.get(1, 0)}")
    print(f"Missing sensor values: {int(X.isna().sum().sum()):,}")
    print(
        "Missing timestamps: "
        f"{int(df['timestamp'].isna().sum())}"
    )
    print(
        "Duplicate timestamps: "
        f"{int(df['timestamp'].duplicated().sum())}"
    )
    print(
        f"Timestamp range: {df['timestamp'].min()} "
        f"to {df['timestamp'].max()}"
    )

    print("\n=== LABEL DISTRIBUTION ===")
    print(label_counts.sort_index())

    print("\n=== MOST INCOMPLETE SENSORS ===")
    print(missing_rate.sort_values(ascending=False).head(10))

    # Export the complete ranking and the selected eight sensors.
    ranking = rank_sensors(X, df["label"])

    ranking_path = REPORT_DIR / "sensor_ranking.csv"
    selected_path = REPORT_DIR / "selected_sensors.csv"

    ranking.to_csv(ranking_path, index=False)

    selected = ranking.head(8).copy()
    selected.to_csv(selected_path, index=False)

    print("\n=== TOP 20 SENSOR CANDIDATES ===")
    print(ranking.head(20).to_string(index=False))

    print("\n=== SELECTED 8 SENSORS ===")
    print(selected.to_string(index=False))

    print("\nReports written to:")
    print(ranking_path)
    print(selected_path)

    print(
        "\nNote: sensor selection uses pass/fail labels as a screening "
        "method. It does not establish causation. Missing values were "
        "imputed only for ranking, not for future SPC calculations."
    )


if __name__ == "__main__":
    main()
