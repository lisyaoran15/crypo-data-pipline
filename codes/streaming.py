import websocket
import json
import polars as pl
import sys
from pathlib import Path
import os
from datetime import datetime
import time
import logging

ROOT_DIR = Path(__file__).resolve().parent.parent  # trỏ về r:\my_prj\
sys.path.append(str(ROOT_DIR))

from codes.schema import SCHEMA

# CẤU HÌNH LOGGING
# Cấu hình định dạng chung cho toàn bộ log xuất ra
logging.basicConfig(level=logging.INFO,
                    format="%(asctime)s | %(levelname)s | %(message)s",
                    datefmt="%Y-%m-%d %H:%M:%S"
                    )
# Khởi tạo một logger riêng cho file này để dễ phân biệt
log = logging.getLogger("StreamingBot")

class RawStreaming:
    """ kline_1m,
        "e": Event type
        "E": Event time
        "s": Symbol
        "t": Thời gian bắt đầu nến
        "T": Thời gian kết thúc nến
        "i": Khung thời gian 1 phút
        "o": Giá mở cửa
        "c": Giá hiện tại/đóng cửa
        "h": Giá cao nhất
        "l": Giá thấp nhất
        "v": Số lượng BTC giao dịch
        "n": Số lượng lệnh khớp
        "x": Nến đã đóng chưa?
        "q": Số lượng USDT giao dịch
        "V": Taker buy base asset volume
        "Q": Taker buy quote asset volume
        "B": Ignore
    """
         
    def __init__(self, symbol="BTCUSDT",  
                 connect_type='kline_1m', 
                 buffer_size=10, 
                 base_path=None):
        
        # 1. CHỐT CHẶN AN TOÀN
        if base_path is None:
            raise ValueError("error: no base path, example: base_path='R:/data/')")
            
        """ define 1 số element quan trọng: 
            symbol: pair giao dịch
            connection_type: loại kết nối với API sàn
            storage: tạo df chứa data
            base_path: path local chứa data
        """
        self.symbol = symbol
        self.connect_type = connect_type
        self.endpoint = f"wss://stream.binance.com:9443/ws/{self.symbol.lower()}@{self.connect_type}"
        self.buffer = []
        self.buffer_size = buffer_size
        self.base_path = base_path
        
        # 2. TẠO THƯ MỤC TỰ ĐỘNG (Chỉ cần 1 dòng này là đủ)
        os.makedirs(self.base_path, exist_ok=True)
        
    def on_message(self, ws, message):
        """ def này dùng để offload data vào local 
            rule đặt tên file: symbol_yyyymmdd.parquet
        """
        msg_dict = json.loads(message) 

        if 'k' not in msg_dict:  
            return
        k = msg_dict['k']

        if k['x']: # nếu x = True -> đây là nến đóng cửa 
            # Chuyển đổi dữ liệu sang đúng kiểu
            row = {"open_time": k['t'],"open": float(k['o']),
                    "high": float(k['h']),"low": float(k['l']),
                    "close": float(k['c']),"volume": float(k['v']),
                    "close_time": k['T'],"quote_volume": float(k['q']),
                    "num_trades": int(k['n']),"taker_buy_base": float(k['V']),
                    "taker_buy_quote": float(k['Q']),"ignore": int(k['B'])
                    }
            self.buffer.append(row)
            
            # Ghi log bằng mức INFO thay vì print
            log.info(f"{self.symbol} - Buffered {len(self.buffer)}/{self.buffer_size}")

            # Nếu đủ số lượng trong buffer thì tiến hành ghi
            if len(self.buffer) >= self.buffer_size:
                self.flush_to_parquet()

    def flush_to_parquet(self):
        """offload về local"""

        # nếu buffer chưa có thì không return gì cả
        if not self.buffer:
            return
        
        first_candle_time = datetime.fromtimestamp(self.buffer[0]['open_time'] / 1000)
        
        # BƯỚC 1: TẠO THƯ MỤC NĂM 
        year_str = str(first_candle_time.year)
        year_folder = os.path.join(self.base_path, year_str)
        os.makedirs(year_folder, exist_ok=True) 
        
        filename = f"{self.symbol}_{first_candle_time.strftime('%Y%m%d')}.parquet"
        
        # Đường dẫn trỏ vào thư mục năm thay vì base_path
        filepath = os.path.join(year_folder, filename) 
        tmp_filepath = filepath + ".tmp" # Đường dẫn file tạm
        
        # Tạo DataFrame từ buffer
        new_df = pl.DataFrame(self.buffer, schema=SCHEMA)
        
        if os.path.exists(filepath):
            # Đọc file cũ và gộp lại
            existing_df = pd.read_parquet(filepath)
            final_df = pd.concat([existing_df, new_df], ignore_index=True)
            final_df.drop_duplicates(subset=['open_time'], keep='last', inplace=True)
        else:
            final_df = pl.concat([existing_df, new_df]).unique(subset=['open_time'], keep='last')
        
        # BƯỚC 2: GHI FILE AN TOÀN (ATOMIC WRITE)
        # Ghi xuống file TẠM trước
        final_df.to_parquet(tmp_filepath)
        
        # Trên Windows, nếu dùng os.rename đè lên file đã có sẵn, nó sẽ báo lỗi.
        # Nên ta cần xóa file cũ (nếu có) trước khi đổi tên file tạm thành file chính thức.
        # Trên Windows, dùng replace để ghi đè an toàn
        if os.path.exists(filepath):
            os.replace(tmp_filepath, filepath)
        else:
            os.rename(tmp_filepath, filepath)
        
        # Ghi log hoàn thành lưu file
        log.info(f"Add {len(self.buffer)} candle in {year_str}/{filename}")
        self.buffer = [] # Reset buffer

    
    def deploy(self):
        log.info(f"Bắt đầu thu thập dữ liệu Raw Streaming cho {self.symbol}...")
        while True:
            try: 
                self.ws = websocket.WebSocketApp(self.endpoint, 
                                            on_message=self.on_message, 
                                            on_error=lambda ws, err: log.error(f"WebSocket error: {err}"),
                                            on_close=lambda ws, status, msg: (log.warning("WS closed, flushing..."), self.flush_to_parquet()),
                                            )
                self.ws.run_forever(ping_interval=20, ping_timeout=10)  
            except Exception as e:
                # Ghi log dạng ERROR nếu sập mạng
                log.error(f"system error: {e}")
            log.warning("reconnect after 5 seconds...")
            time.sleep(5)
           