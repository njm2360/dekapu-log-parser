import json
import logging
from pathlib import Path


class FileOffsetStore:
    def __init__(self, filepath: Path):
        self.filepath = filepath
        self.offsets: dict[str, int] = self._load()
        logging.info(
            f"[OffsetStore] Initialized with file={self.filepath}, "
            f"loaded {len(self.offsets)} entries"
        )

    def _load(self) -> dict[str, int]:
        if self.filepath.exists():
            try:
                with open(self.filepath, "r", encoding="utf-8") as f:
                    data = json.load(f)
                    logging.info(f"[OffsetStore] Loaded offsets from {self.filepath}")
                    return data
            except Exception as e:
                logging.warning(f"[OffsetStore] Failed to load offsets: {e}")
        else:
            logging.info(f"[OffsetStore] Offset file not found, starting empty")
        return {}

    def save(self):
        try:
            with open(self.filepath, "w", encoding="utf-8") as f:
                json.dump(self.offsets, f, ensure_ascii=False, indent=2)
            logging.info(
                f"[OffsetStore] Saved {len(self.offsets)} offsets to {self.filepath}"
            )
        except Exception as e:
            logging.error(f"[OffsetStore] Failed to save offsets: {e}")
