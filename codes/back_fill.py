import time
import requests
import pandas as pd
import polars as pl
from pathlib import Path
import sys
import logging
import re

ROOT_DIR = Path(__file__).resolve().parent.parent  # trỏ về r:\my_prj\
sys.path.append(str(ROOT_DIR))

from codes.schema import SCHEMA

# Logging
logging.basicConfig(level=logging.INFO,format="%(asctime)s | %(levelname)s | %(message)s",)
log = logging.getLogger("BinanceCollector")

# Class
class Backfill:

    # init define
    def __init__(self, symbol: str, interval: str, save_path: str, file_type="parquet", tz="UTC",):
        self.symbol = symbol.upper()
        self.interval = interval.lower()
        self.path = Path(save_path)
        self.file_type = file_type
        self.tz = tz # timezone
        self._validate_interval()
        self._calc_step()
        self.url = "https://api.binance.com/api/v3/klines"
        self.path.mkdir(parents=True, exist_ok=True)

    # init validation
    def _validate_interval(self):
        if not re.fullmatch(r'\d+[smhd]', self.interval):
            raise ValueError(f"Invalid interval: {self.interval}")

    # step calculation
    def _calc_step(self):
        unit_seconds = {"s": 1, "m": 60, "h": 3600, "d": 86400}
        value = int(self.interval[:-1])
        unit = self.interval[-1]
        self.step_sec = value * unit_seconds[unit]
        self.step_ms = self.step_sec * 1000

    # batch calculation
    def calc_missing_days(self, start, end):
        days = pd.date_range(start=start, end=end, freq="D", tz=self.tz)
        missing = []

        for d in days:
            name = f"{self.symbol}_{d.strftime('%Y%m%d')}.{self.file_type}"
            year_folder = self.path / str(d.year)
            filepath = year_folder / name  # ← thêm dòng này

            if not filepath.exists():
                missing.append(d)
            elif filepath.stat().st_size < 50 * 1024:
                missing.append(d)

        return missing

    # API 
    def _request(self, params, retry=5):
        for i in range(retry):
            try:
                r = requests.get(self.url,params=params,timeout=15,)
                if r.status_code != 200:
                    raise Exception(r.text)
                return r.json()
            
            except Exception as e:
                log.warning(f"Retry {i+1}/{retry} : {e}")
                time.sleep(2)

        raise RuntimeError("api failed permanently")

    # collect 
    def collect_day(self, day: pd.Timestamp):
        log.info(f"Collecting {day.date()}")
        start = day.normalize()
        end = start + pd.Timedelta(days=1)
        start_ms = int(start.timestamp() * 1000)
        end_ms = int(end.timestamp() * 1000)
        
        data = []
        current = start_ms

        while current < end_ms:
            params = {
                "symbol": self.symbol,
                "interval": self.interval,
                "startTime": current,
                "endTime": end_ms - 1, # Lấy sát nút cuối ngày
                "limit": 1000,
            }
            resp = self._request(params)

            if not resp: break
            data.extend(resp)

            last_open = resp[-1][0]
            if last_open <= current: break           
           
            current = last_open + self.step_ms
            time.sleep(0.2) 

        if not data:
            log.warning("No data collected")
            return

        # 1. Tạo DF thô từ list of lists
        df = pl.DataFrame(data, schema=SCHEMA, orient="row")
        
        # 2. Ép kiểu hàng loạt cực nhanh bằng SCHEMA (Xử lý đủ 12 cột)
        df = df.with_columns([
            pl.col(col).cast(dtype) for col, dtype in SCHEMA.items()
        ])

        # 3. Filter để đảm bảo dữ liệu nằm trọn trong ngày (Phòng hờ API trả dư)
        df = df.filter((pl.col("open_time") >= start_ms) & (pl.col("open_time") < end_ms))

        self._validate_data(df, start, end)
        self._save(df, day)


    # validation 
    def _validate_data(self, df, start, end):

        expected = int((end - start).total_seconds() / self.step_sec)

        real = df.height

        ratio = real / expected

        log.info(f"Candle: {real}/{expected} ({ratio:.2%})")

        if ratio < 0.99:
            log.warning("data possibly incomplete")


    # save
    def _save(self, df, day):
        # Lấy năm từ biến day để làm tên thư mục
        year_folder = self.path / str(day.year)

        # Tạo thư mục nếu chưa có (exist_ok=True giúp không báo lỗi nếu folder đã tồn tại)
        year_folder.mkdir(parents=True, exist_ok=True)

        name = f"{self.symbol}_{day.strftime('%Y%m%d')}.{self.file_type}"

        final = year_folder / name
        tmp = final.with_suffix(".tmp")

        if self.file_type == "parquet":
            df.write_parquet(tmp)

        elif self.file_type == "csv":
            df.write_csv(tmp)

        else:
            raise ValueError("unsupported format")

        tmp.replace(final)

        log.info(f"saved {final.name} into {year_folder.name}/")


    # pipeline 
    def run(self, start, end):

        days = self.calc_missing_days(start, end)
        log.info(f"Missing days: {len(days)}")

        for d in days:
            self.collect_day(d)
            time.sleep(1)