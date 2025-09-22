import asyncio
import logging
from pathlib import Path
from datetime import datetime, timedelta

from app.analysis.log_parser import MppLogParser
from app.utils.offset_store import FileOffsetStore
from app.utils.influxdb import InfluxWriterAsync


class VRChatLogWatcher:
    def __init__(
        self, log_dir: Path, influx: InfluxWriterAsync, offset_store: FileOffsetStore
    ):
        self.log_dir = log_dir
        self.influx = influx
        self.offset_store = offset_store
        self.parsers: dict[str, MppLogParser] = {}

        logging.info(f"[Watcher] Initialized. Log directory={log_dir}")

    async def watch_file(self, log_file: Path):
        fname = log_file.name
        parser = self.parsers.setdefault(fname, MppLogParser(fname))
        offset = self.offset_store.offsets.get(fname)

        logging.info(f"[Watcher] Start watching file={fname}, offset={offset}")

        with open(log_file, "r", encoding="utf-8", errors="ignore") as f:
            if offset is not None:
                f.seek(offset)
                logging.info(f"[Watcher] Resumed from offset {offset} ({fname})")
            else:
                f.seek(0, 2)
                logging.info(f"[Watcher] Skip to EOF (no offset) ({fname})")

            last_activity = datetime.now()

            while True:
                line = f.readline()
                if not line:
                    await asyncio.sleep(0.5)
                    if datetime.now() - last_activity > timedelta(hours=1):
                        logging.info(f"[Watcher] Stop watching {fname}")
                        break
                    continue

                self.offset_store.offsets[fname] = f.tell()
                last_activity = datetime.now()

                point = parser.parse_line(line.strip())
                if point:
                    await self.influx.write(point)
                    logging.info(f"[Watcher] Data write OK ({fname})")

    async def run(self):
        tasks: dict[Path, asyncio.Task] = {}
        logging.info(f"[Watcher] Start main loop (dir={self.log_dir})")

        while True:
            for log_file in self.log_dir.glob("output_log_*.txt"):
                if not log_file.is_file():
                    continue
                if log_file.name not in tasks or tasks[log_file.name].done():
                    logging.info(f"[Watcher] New monitoring task: {log_file}")
                    tasks[log_file.name] = asyncio.create_task(
                        self.watch_file(log_file)
                    )

            await asyncio.sleep(10)
