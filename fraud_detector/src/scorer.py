"""Loading and scoring a versioned model artifact bundle."""

from __future__ import annotations

import json
import logging
import os
from pathlib import Path

import joblib
import pandas as pd
from catboost import CatBoostClassifier

from preprocessing import transform

logger = logging.getLogger(__name__)


class FraudScorer:
    """Model, preprocessing state and metadata loaded from one model version."""

    def __init__(self, artifact_root: str | Path, model_version: str | None = None):
        artifact_root = Path(artifact_root)
        self.model_version = model_version or self._find_latest_version(artifact_root)
        self.artifact_dir = artifact_root / self.model_version
        self._validate_artifacts()
        self.preprocessor = joblib.load(self.artifact_dir / "preprocessor.joblib")
        with (self.artifact_dir / "metadata.json").open(encoding="utf-8") as file:
            self.metadata = json.load(file)

        artifact_version = self.metadata.get("model_version")
        if artifact_version != self.model_version:
            raise ValueError(
                f"Версия в metadata.json ({artifact_version!r}) не совпадает с выбранной версией "
                f"({self.model_version!r})"
            )

        self.threshold = float(self.metadata["threshold"])
        self.model = CatBoostClassifier()
        self.model.load_model(self.artifact_dir / self.metadata.get("model_file", "model.cbm"))
        logger.info("Loaded model version %s from %s", self.model_version, self.artifact_dir)

    @classmethod
    def from_environment(cls) -> "FraudScorer":
        return cls(
            artifact_root=os.getenv("ARTIFACT_DIR", "/app/artifacts"),
            model_version=os.getenv("MODEL_VERSION") or None,
        )

    def predict(self, raw_transactions: pd.DataFrame, source_info: str = "kafka") -> pd.DataFrame:
        features = transform(raw_transactions, self.preprocessor)
        scores = self.model.predict_proba(features)[:, 1]
        logger.info(
            "Prediction complete for %s: model_version=%s, rows=%d",
            source_info,
            self.model_version,
            len(raw_transactions),
        )
        return pd.DataFrame(
            {
                "score": scores,
                "fraud_flag": (scores >= self.threshold).astype(int),
                "model_version": self.model_version,
            },
            index=raw_transactions.index,
        )

    def _validate_artifacts(self) -> None:
        required = ["model.cbm", "preprocessor.joblib", "metadata.json"]
        missing = [name for name in required if not (self.artifact_dir / name).is_file()]
        if missing:
            raise FileNotFoundError(
                f"Версия модели {self.model_version!r} неполная в {self.artifact_dir}: отсутствуют {missing}"
            )

    @staticmethod
    def _find_latest_version(artifact_root: Path) -> str:
        if not artifact_root.is_dir():
            raise FileNotFoundError(f"Каталог артефактов не найден: {artifact_root}")

        candidates = []
        for directory in artifact_root.iterdir():
            metadata_path = directory / "metadata.json"
            if not directory.is_dir() or not metadata_path.is_file():
                continue
            if not (directory / "model.cbm").is_file() or not (directory / "preprocessor.joblib").is_file():
                logger.warning("Skipping incomplete artifact directory %s", directory)
                continue
            try:
                with metadata_path.open(encoding="utf-8") as file:
                    metadata = json.load(file)
                version = metadata.get("model_version")
                if version != directory.name:
                    logger.warning("Skipping inconsistent artifact directory %s", directory)
                    continue
                created_at = metadata.get("created_at_utc", "")
                candidates.append((created_at, directory.name))
            except (OSError, json.JSONDecodeError):
                logger.warning("Skipping unreadable metadata: %s", metadata_path)

        if not candidates:
            raise FileNotFoundError(f"В {artifact_root} нет ни одной версии модели с metadata.json")

        _, latest_version = max(candidates)
        logger.info("MODEL_VERSION is not set; selected latest version %s", latest_version)
        return latest_version
