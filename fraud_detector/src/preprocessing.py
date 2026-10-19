"""Shared preprocessing used by both training and online inference."""

from __future__ import annotations

from typing import Any

import numpy as np
import pandas as pd

TARGET = "target"
CATEGORICAL_COLUMNS = ["gender", "merch", "cat_id", "one_city", "us_state", "jobs"]
NUMERIC_COLUMNS = ["amount", "population_city", "lat", "lon", "merchant_lat", "merchant_lon"]
RAW_FEATURE_COLUMNS = ["transaction_time", *CATEGORICAL_COLUMNS, *NUMERIC_COLUMNS]
MODEL_CATEGORICAL_COLUMNS = [*CATEGORICAL_COLUMNS, "hour", "month", "day_of_week"]
MODEL_NUMERIC_COLUMNS = ["amount_log", "population_city_log", "distance_km_log"]
MODEL_FEATURE_COLUMNS = [*MODEL_CATEGORICAL_COLUMNS, *MODEL_NUMERIC_COLUMNS]


def fit_preprocessor(frame: pd.DataFrame) -> dict[str, Any]:
    """Fit the state required by preprocessing."""
    _validate_columns(frame, RAW_FEATURE_COLUMNS, "обучающих")
    numeric = frame[NUMERIC_COLUMNS].apply(pd.to_numeric, errors="coerce")
    medians = numeric.median().fillna(0.0).to_dict()
    return {
        "artifact_type": "fraud_preprocessor",
        "artifact_version": 1,
        "raw_feature_columns": RAW_FEATURE_COLUMNS,
        "categorical_columns": CATEGORICAL_COLUMNS,
        "numeric_columns": NUMERIC_COLUMNS,
        "numeric_medians": {key: float(value) for key, value in medians.items()},
        "model_categorical_columns": MODEL_CATEGORICAL_COLUMNS,
        "model_feature_columns": MODEL_FEATURE_COLUMNS,
        "unknown_category": "__UNKNOWN__",
    }


def transform(frame: pd.DataFrame, preprocessor: dict[str, Any]) -> pd.DataFrame:
    """Transform raw transactions according to a fitted preprocessing artifact."""
    _validate_preprocessor(preprocessor)
    _validate_columns(frame, preprocessor["raw_feature_columns"], "входных")

    result = pd.DataFrame(index=frame.index)
    unknown = preprocessor["unknown_category"]
    for column in preprocessor["categorical_columns"]:
        result[column] = frame[column].astype("string").fillna(unknown).astype(str)

    transaction_time = pd.to_datetime(frame["transaction_time"], errors="coerce")
    result["hour"] = transaction_time.dt.hour.fillna(-1).astype(int).astype(str)
    result["month"] = transaction_time.dt.month.fillna(-1).astype(int).astype(str)
    result["day_of_week"] = transaction_time.dt.dayofweek.fillna(-1).astype(int).astype(str)

    numeric = frame[preprocessor["numeric_columns"]].apply(pd.to_numeric, errors="coerce")
    numeric = numeric.fillna(preprocessor["numeric_medians"])
    lat1, lon1 = np.radians(numeric["lat"]), np.radians(numeric["lon"])
    lat2, lon2 = np.radians(numeric["merchant_lat"]), np.radians(numeric["merchant_lon"])
    dlat, dlon = lat2 - lat1, lon2 - lon1
    haversine = np.sin(dlat / 2) ** 2 + np.cos(lat1) * np.cos(lat2) * np.sin(dlon / 2) ** 2
    distance_km = 2 * 6371.0088 * np.arcsin(np.sqrt(np.clip(haversine, 0, 1)))

    result["amount_log"] = np.log1p(numeric["amount"].clip(lower=0)).astype(float)
    result["population_city_log"] = np.log1p(numeric["population_city"].clip(lower=0)).astype(float)
    result["distance_km_log"] = np.log1p(distance_km).astype(float)
    return result[preprocessor["model_feature_columns"]]


def _validate_columns(frame: pd.DataFrame, required: list[str], kind: str) -> None:
    missing = sorted(set(required) - set(frame.columns))
    if missing:
        raise ValueError(f"В {kind} данных отсутствуют колонки: {missing}")


def _validate_preprocessor(preprocessor: dict[str, Any]) -> None:
    if preprocessor.get("artifact_type") != "fraud_preprocessor":
        raise ValueError("Неизвестный формат preprocessor.joblib")
    if preprocessor.get("artifact_version") != 1:
        raise ValueError(
            f"Неподдерживаемая версия препроцессора: {preprocessor.get('artifact_version')}"
        )
