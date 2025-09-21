import os
import json
import logging
import asyncio
from pathlib import Path
from collections import deque
from typing import Deque, Tuple
from datetime import datetime, timedelta, timezone
from urllib.parse import urlparse, parse_qs, unquote

from dotenv import load_dotenv
from influxdb_client import Point, WritePrecision
from influxdb_client.rest import ApiException
from influxdb_client.client.influxdb_client_async import InfluxDBClientAsync


load_dotenv()

INFLUX_URL = os.getenv("INFLUX_URL")
INFLUX_TOKEN = os.getenv("INFLUX_TOKEN")
ORG = os.getenv("INFLUX_ORG")
BUCKET = os.getenv("INFLUX_BUCKET")


# ログ設定
local_log_dir = Path("logs")
local_log_dir.mkdir(parents=True, exist_ok=True)
start_time = datetime.now().strftime("%Y%m%d_%H%M%S")
local_log_file = local_log_dir / f"{start_time}.log"

logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s [%(levelname)s] %(message)s",
    datefmt="%Y-%m-%d %H:%M:%S",
    handlers=[
        logging.FileHandler(local_log_file, encoding="utf-8"),
        logging.StreamHandler(),
    ],
    force=True,
)


class InfluxWriterAsync:
    def __init__(self, url: str, token: str, org: str, bucket: str):
        self.url = url
        self.token = token
        self.org = org
        self.bucket = bucket
        self.client = InfluxDBClientAsync(url=url, token=token, org=org, timeout=5000)
        self.write_api = self.client.write_api()

    async def write(self, point: Point):
        try:
            await self.write_api.write(bucket=self.bucket, org=self.org, record=point)
        except ApiException as e:
            logging.error(f"InfluxDB write failed: {e}")
            raise

    async def close(self):
        await self.client.__aexit__(None, None, None)


class VRChatLogWatcher:
    def __init__(self, influx: InfluxWriterAsync):
        self.influx = influx
        self.vrchat_log_dir = Path.home() / "AppData" / "LocalLow" / "VRChat" / "VRChat"
        self.credit_history: Deque[Tuple[datetime, int]] = deque()

    def get_latest_log(self) -> Path:
        log_files = sorted(
            self.vrchat_log_dir.glob("output_log_*.txt"),
            key=lambda f: f.stat().st_mtime,
        )
        if not log_files:
            raise FileNotFoundError("VRChat log file not found")
        return log_files[-1]

    async def run(self):
        log_file = self.get_latest_log()
        logging.info(f"Monitoring log file: {log_file}")

        with open(log_file, "r", encoding="utf-8", errors="ignore") as f:
            f.seek(0, 2)  # 既存部分は無視

            while True:
                line = f.readline()
                if not line:
                    await asyncio.sleep(0.5)
                    continue

                if "https://push.trap.games/api/v3/data" not in line:
                    continue

                await self.process_line(line.strip())

    async def process_line(self, line: str):
        try:
            parsed = urlparse(line)
            query = parse_qs(parsed.query)

            raw_data = unquote(query.get("data", ["{}"])[0])
            try:
                data = json.loads(raw_data)
            except json.JSONDecodeError as e:
                logging.warning(f"JSON decode error: {e} | raw={raw_data[:100]}...")
                return

            user_id = query.get("user_id", [""])[0]
            credit_all = data.get("credit_all")

            if credit_all is not None:
                # 履歴に追加
                now = datetime.now(timezone.utc)
                self.credit_history.append((now, credit_all))

                # 2分より古いデータは削除(データ飛び対策)
                while self.credit_history and self.credit_history[0][0] < (
                    now - timedelta(minutes=2)
                ):
                    self.credit_history.popleft()

                # 線形補間で1分前の値を求める
                one_minute_ago = now - timedelta(minutes=1)
                before = None
                after = None

                for t, c in self.credit_history:
                    if t <= one_minute_ago:
                        before = (t, c)
                    if t >= one_minute_ago and after is None:
                        after = (t, c)

                if before and after:
                    t0, c0 = before
                    t1, c1 = after
                    if t1 > t0:  # ゼロ除算防止
                        ratio = (one_minute_ago - t0).total_seconds() / (
                            t1 - t0
                        ).total_seconds()
                        interpolated_credit = c0 + (c1 - c0) * ratio
                        delta = int(credit_all - interpolated_credit)
                    else:
                        delta = None
                else:
                    delta = None
            else:
                delta = None

            p = (
                Point("mpp-savedata")
                .tag("user", user_id)
                .time(datetime.now(timezone.utc), WritePrecision.NS)
            )

            for k, v in data.items():
                if isinstance(v, (int, float, str)):
                    p = p.field(k, v)
                elif isinstance(v, dict) and k.startswith("dc_"):
                    for sub_k, sub_v in v.items():
                        if isinstance(sub_v, (int, float, str)):
                            field_name = f"{k}_{sub_k}"
                            p = p.field(field_name, sub_v)

            if delta is not None:
                p = p.field("credit_all_delta_1m", delta)
                logging.info(delta)

            await self.influx.write(p)
            logging.info(f"Data write success (user={user_id})")

        except Exception as e:
            logging.exception(f"Unexpected error while processing line: {e}")


async def main():
    influx = InfluxWriterAsync(INFLUX_URL, INFLUX_TOKEN, ORG, BUCKET)
    watcher = VRChatLogWatcher(influx)

    try:
        await watcher.run()
    finally:
        await influx.close()


if __name__ == "__main__":
    asyncio.run(main())
