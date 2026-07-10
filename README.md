# Financial Data Pipeline

An end-to-end data pipeline for collecting, processing, and transforming financial market data at scale. Built with Python, Apache Airflow, and Docker.

## Overview

This project implements a production-grade ETL pipeline that handles both historical backfill and real-time streaming of financial OHLCV (Open-High-Low-Close-Volume) data. Designed for reliability, scalability, and minimal manual intervention.

## Architecture

```
┌─────────────────────────────────────────────────────┐
│                   Apache Airflow                    │
│              (Orchestration Layer)                  │
├──────────────────┬──────────────────────────────────┤
│                  │                                  │
│   ┌──────────┐   │   ┌──────────────┐               │
│   │ Backfill │   │   │  Streaming   │               │
│   │   DAG    │   │   │    DAG       │               │
│   └────┬─────┘   │   └──────┬───────┘               │
│        │         │          │                       │
│        ▼         │          ▼                       │
│   ┌──────────────────────────────┐                  │
│   │      Data Collection         │                  │
│   │   (Binance REST / WebSocket) │                  │
│   └──────────────┬───────────────┘                  │
│                  │                                  │
│                  ▼                                  │
│   ┌──────────────────────────────┐                  │
│   │      Resampling Layer        │                  │
│   │    (1min → 30min candles)    │                  │
│   └──────────────┬───────────────┘                  │
│                  │                                  │
│                  ▼                                  │
│   ┌──────────────────────────────┐                  │
│   │      Feature ETL             │                  │
│   │  (Technical Indicators)      │                  │
│   └──────────────┬───────────────┘                  │
│                  │                                  │
│                  ▼                                  │
│   ┌──────────────────────────────┐                  │
│   │      Storage Layer           │                  │
│   │  (Parquet / Columnar Store)  │                  │
│   └──────────────────────────────┘                  │
└─────────────────────────────────────────────────────┘
```

## Key Features

**Data Collection**
- Historical backfill from 2017 to present via REST API
- Real-time streaming for continuous data ingestion
- Configurable symbol and timeframe support

**Processing**
- Raw 1-minute OHLCV data resampled to 30-minute candles
- Feature computation: SMA, EMA across multiple lookback windows
- Built with Polars for high-performance DataFrame operations

**Orchestration**
- Apache Airflow DAGs for scheduling and monitoring
- Separate DAGs for backfill (one-time) and streaming (continuous)
- Auto-start on system boot via Docker
- Built-in retry logic and failure alerting

**Storage**
- Parquet file format for efficient columnar storage and compression
- Partitioned by date for fast range queries
- Designed for NAS / networked storage compatibility

## Tech Stack

| Component           | Technology               | 
| Language            | Python 3.11+             |
| Data Processing     | Polars                   |
| Orchestration       | Apache Airflow 2.x       |
| Containerization    | Docker & Docker Compose  |
| Data Source         | Binance API              |
| Storage Format      | Apache Parquet           |
| Feature Engineering | Custom indicators module |

## Project Structure

```
.
├── dags/
│   ├── backfill_dag.py          # Historical data backfill
│   ├── streaming_dag.py         # Real-time data ingestion
│   └── feature_etl_dag.py       # Feature computation pipeline
├── src/
│   ├── collectors/
│   │   ├── rest_collector.py    # REST API data collection
│   │   └── ws_collector.py      # WebSocket streaming
│   ├── processing/
│   │   ├── resampler.py         # OHLCV resampling logic
│   │   └── features.py          # Technical indicator computation
│   ├── storage/
│   │   └── parquet_writer.py    # Parquet I/O utilities
│   └── config/
│       └── settings.py          # Configuration management
├── docker-compose.yml
├── Dockerfile
├── requirements.txt
└── README.md
```

## Feature Engineering

The pipeline computes a configurable set of technical indicators on resampled data:

| Feature | Type                       | Lookback Windows        |
|---------|----------------------------|-------------------------|
| SMA     | Simple Moving Average      | 5, 10, 20, 50, 100, 200 |
| EMA     | Exponential Moving Average | 5, 10, 20, 50, 100, 200 |
