# deploy_airflow.py
import polars as pl
from pathlib import Path
from datetime import datetime, timedelta
import logging

log = logging.getLogger(__name__)


def task_backfill(symbol, interval, raw_path):
    from codes.back_fill import Backfill
    collector = Backfill(symbol, interval, raw_path)
    collector.run("2017-01-01", datetime.now().date())

def task_etl_full(raw_path, feature_path, symbol, windows, momentum_windows):
    from codes.ETL.feature_list import MA, momentum, macd, dmi
    from codes.ETL.ETL import FeaturePipeline

    # collect features
    features = [MA(window=w, type="simple") for w in windows] + \
               [MA(window=w, type="exponential") for w in windows] + \
               [momentum(window= w) for w in momentum_windows] + \
               [macd()] +  \
               [dmi()]
    pipeline = FeaturePipeline(features)

    # calculate extra days
    max_window = max(max(windows), max(momentum_windows), 26, 14)
    extra_days = (max_window // 1440) + 1
    log.info(f"max window:{max_window}, loading {extra_days}, extra days for lookback")

    # define raw path and feature path
    raw = Path(raw_path)
    feature = Path(feature_path)

    raw_files = {f.name: f for f in raw.rglob("*.parquet")}
    interim_files = {f.name: f for f in feature.rglob("*.parquet")}

    missing_files = {}

    # check interim mising files
    for name, raw_file in raw_files.items():
        if name not in interim_files:
            missing_files[name] = raw_file
        # check interim size invalid
        elif interim_files[name].stat().st_size < raw_file.stat().st_size * 0.5:
            missing_files[name] = raw_file

    if not missing_files:
        log.info("mo missing days, skipping full etl")
        return

    log.info(f"Missing {len(missing_files)} days, running etl")

    for filename, raw_file in sorted(missing_files.items()):
        # create name
        day_str = filename.split("_")[1].split(".")[0]
        day = datetime.strptime(day_str, "%Y%m%d").date()

        # calculate days
        yesterday = day - timedelta(days=1)
        yesterday_file = raw / str(yesterday.year) / f"{symbol}_{yesterday.strftime('%Y%m%d')}.parquet"

        # define files to load
        files_to_load = [str(raw_file)]
        for i in range(1, extra_days + 1):
            prev_day = day - timedelta(days=i)
            prev_file = raw / str(prev_day.year) / f"{symbol}_{prev_day.strftime('%Y%m%d')}.parquet"
            if prev_file.exists():
                files_to_load.insert(0, str(prev_file))

        # create files and sort
        df = pl.concat([pl.read_parquet(p) for p in files_to_load]).sort("open_time")
        df = pipeline.feature_engine(df)

        day_ms_start = int(datetime.combine(day, datetime.min.time()).timestamp() * 1000)
        day_ms_end = int(datetime.combine(day + timedelta(days=1), datetime.min.time()).timestamp() * 1000)
        df = df.filter((pl.col("open_time") >= day_ms_start) &
                       (pl.col("open_time") < day_ms_end))

        year_folder = feature / str(day.year)
        year_folder.mkdir(parents=True, exist_ok=True)
        df.write_parquet(year_folder / filename)
        log.info(f"Saved {filename}")

    log.info("full etl done")


def task_etl_incremental(raw_path, feature_path, symbol, windows, momentum_windows):
    from codes.ETL.feature_list import MA, momentum, macd, dmi
    from codes.ETL.ETL import FeaturePipeline
    from datetime import date

    features = [MA(window=w, type="simple") for w in windows] + \
               [MA(window=w, type="exponential") for w in windows] + \
               [momentum(window= w) for w in momentum_windows] + \
               [macd()] + [dmi()]
    pipeline = FeaturePipeline(features)

    # Tính max window
    max_window = max(max(windows), max(momentum_windows), 26, 14)
    extra_days = (max_window // 1440) + 1

    today = date.today()
    yesterday = today - timedelta(days=1)
    raw = Path(raw_path)

    files_to_load = []
    for i in range(extra_days, 0, -1):
        prev_day = yesterday - timedelta(days=i)
        prev_file = raw / str(prev_day.year) / f"{symbol}_{prev_day.strftime('%Y%m%d')}.parquet"
        if prev_file.exists():
            files_to_load.append(str(prev_file))
    
    # Thêm hôm qua và hôm nay
    for d in [yesterday, today]:
        f = raw / str(d.year) / f"{symbol}_{d.strftime('%Y%m%d')}.parquet"
        if f.exists():
            files_to_load.append(str(f))

    if not files_to_load:
        log.warning("no raw files found for incremental etl")
        return
    
    df = pl.concat([pl.read_parquet(p) for p in files_to_load]).sort("open_time")
    df = pipeline.feature_engine(df)

    yesterday_ms_start = int(datetime.combine(yesterday, datetime.min.time()).timestamp() * 1000)
    yesterday_ms_end = int(datetime.combine(today, datetime.min.time()).timestamp() * 1000)
    df = df.filter(
        (pl.col("open_time") >= yesterday_ms_start) &
        (pl.col("open_time") < yesterday_ms_end)
    )

    feature = Path(feature_path)
    year_folder = feature / str(yesterday.year)
    year_folder.mkdir(parents=True, exist_ok=True)
    filename = f"{symbol}_{yesterday.strftime('%Y%m%d')}.parquet"
    df.write_parquet(year_folder / filename)
    log.info(f"Saved {filename}")