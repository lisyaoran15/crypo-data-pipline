import polars as pl
import numpy as np
import sys
from pathlib import Path
import logging

ROOT_DIR = Path(__file__).resolve().parent.parent
sys.path.append(str(ROOT_DIR))

from codes.ETL.ETL import BaseFeature

log = logging.getLogger(__name__)
class MA(BaseFeature): 
    def __init__(self, window=20, type="simple", cal_col="close"):
        if not isinstance(window, int) or window <= 0:
            raise ValueError("window must be positive int")
        
        if type not in ["simple", "exponential"]:
            raise ValueError("type must be 'simple' or 'exponential'")
             
        self.window = window
        self.type = type
        self.cal_col = cal_col

    def compute(self, df):
        if self.cal_col not in df.columns:
            raise KeyError(f"{self.cal_col} not found in df")
        
        log.info(f"Computing {self.type} MA window={self.window}")
        
        if self.type == "simple":
            result = df.with_columns(
                pl.col(self.cal_col).rolling_mean(window_size=self.window).alias(f"SMA_{self.window}")
            )
            log.info(f"Result columns: {result.columns}")
            return result
        elif self.type == "exponential":
            return df.with_columns(
                pl.col(self.cal_col).ewm_mean(span=self.window, adjust=False).alias(f"EMA_{self.window}")
            )

class momentum(BaseFeature): 
    def __init__(self, window=20, cal_col="close"):
        if not isinstance(window, int) or window <= 0:
            raise ValueError("window must be positive int")
                    
        self.window = window
        self.cal_col = cal_col

    def compute(self, df):
        if self.cal_col not in df.columns:
            raise KeyError(f"{self.cal_col} not found in df")

        log.info(f"Computing momentum window={self.window}")

        result = df.with_columns(
                (pl.col(self.cal_col) - pl.col(self.cal_col).shift(self.window)).alias(f"MMT_{self.window}")
            )
        log.info(f"Result columns: {result.columns}")
        return result
    
class macd(BaseFeature): 
    def __init__(self,s_window= 12, l_window=26, cal_col="close"):
        if not isinstance(s_window, int) or not isinstance(l_window, int) or s_window <= 0 or l_window <= 0:
            raise ValueError("l_window and s_window must be positive int")
                    
        self.s_window = s_window
        self.l_window = l_window
        self.cal_col = cal_col

    def compute(self, df):
        if self.cal_col not in df.columns:
            raise KeyError(f"{self.cal_col} not found in df")

        log.info(f"Computing MACD s_window={self.s_window}, l_window={self.l_window}")

        # calculate MACD line
        result = df.with_columns((pl.col(self.cal_col).ewm_mean(span=self.s_window, adjust=False) - pl.col(self.cal_col).ewm_mean(span=self.l_window, adjust=False)).alias(f"MACD_{self.s_window}{self.l_window}"))
        result = result.with_columns(pl.col(f"MACD_{self.s_window}{self.l_window}").ewm_mean(span=9, adjust=False).alias(f"ema9_MACD_{self.s_window}{self.l_window}"))
        log.info(f"Result columns: {result.columns}")
        return result
    
class dmi(BaseFeature): 
    def __init__(self,window= 11, high_col="high", low_col="low", close_col = "close"):
        if not isinstance(window, int) or window <= 0:
            raise ValueError("window must be positive int")
                    
        self.window = window
        self.high_col = high_col
        self.low_col = low_col
        self.close_col = close_col        

    def compute(self, df):
        if any(col not in df.columns for col in [self.high_col, self.low_col, self.close_col]):
            raise KeyError("Missing required columns")

        log.info(f"Computing DMI window={self.window}")

        # TR = max(high-low, |high-prev_close|, |low-prev_close|)
        df = df.with_columns([pl.col("high").shift(1).alias("prev_high"),
                              pl.col("low").shift(1).alias("prev_low"),
                              pl.col("close").shift(1).alias("prev_close"),])

        df = df.with_columns(pl.max_horizontal(pl.col("high") - pl.col("low"),
                                               (pl.col("high") - pl.col("prev_close")).abs(),
                                               (pl.col("low") - pl.col("prev_close")).abs()).alias("TR"))
        # upmove, downmove
        df = df.with_columns((pl.col("high")- pl.col("prev_high")).alias(f"upmove"))     
        df = df.with_columns((pl.col("low")- pl.col("prev_low")).alias(f"downmove"))

        # plus, minus DM
        df = df.with_columns(pl.when( 
                                    (pl.col(f"upmove") > pl.col(f"downmove")) & (pl.col(f"upmove") > 0)
                                 ).then(pl.col(f"upmove")).otherwise(0).alias("plus_DM"))
        df = df.with_columns(pl.when( 
                                    (pl.col(f"downmove") > pl.col(f"upmove")) & (pl.col(f"downmove") > 0)
                                 ).then(pl.col(f"downmove")).otherwise(0).alias("minus_DM"))
        
        # Smoothed_TR14 = prev_TR14 - (prev_TR14 / 14) + TR
        # Wilder's smoothing ≈ EWM với alpha = 1/14
        df = df.with_columns([
            pl.col("TR").ewm_mean(alpha=1/14, adjust=False).alias(f"TR_{self.window}"),
            pl.col("plus_DM").ewm_mean(alpha=1/14, adjust=False).alias(f"plus_DM_{self.window}"),
            pl.col("minus_DM").ewm_mean(alpha=1/14, adjust=False).alias(f"minus_DM_{self.window}"),
        ])

        # DI
        df = df.with_columns([(pl.col(f"plus_DM_{self.window}") / pl.col(f"TR_{self.window}") * 100).alias("plus_DI"),
                              (pl.col(f"minus_DM_{self.window}") / pl.col(f"TR_{self.window}") * 100).alias("minus_DI"),
                            ])
        # DX
        df = df.with_columns(((pl.col("plus_DI") - pl.col("minus_DI")).abs() /
                              (pl.col("plus_DI") + pl.col("minus_DI")) * 100).alias("DX")
                            )
        # ADX
        df = df.with_columns(pl.col("DX").ewm_mean(alpha=1/14, adjust=False).alias("ADX"))

        # drop cols
        result = df.drop(["prev_high", "prev_low", "prev_close",
                              "TR", "upmove", "downmove",f"TR_{self.window}", 
                              f"plus_DM_{self.window}", f"minus_DM_{self.window}", 
                              "plus_DI", "minus_DI", "DX"])
        log.info(f"Result columns: {result.columns}")
        return result