import yfinance as yf
import pandas as pd
import time
from monitoring.logging_config import setup_logger
logger=setup_logger()
def fetch_stock_data(sector_data,interval="5m",retry_count=3):
    all_data={}
    for sector,symbol in sector_data.items():
        batch_size=8
        for i in range(0,len(symbol),batch_size):
            batch=symbol[i:i+batch_size]
            for attempt in range(retry_count):
                try:
                    logger.info(f"try to fetch batch number {i+1}")
                    data=yf.download(
                        tickers=batch,
                        period="1d",
                        interval=interval,
                        group_by='ticker',
                        progress=False,
                        threads=True
                    )
                    for sym in batch:
                        if not data.empty and hasattr(data.columns, 'levels'):
                            if sym in data.columns.levels[1]:
                                df = data[sym].copy()
                                df['sector'] = sector
                                all_data[sym] = df
                    logger.info(f"Successfully fetched batch number {i+1} and {len(batch)} symbols")
                    time.sleep(2)
                    break
                except Exception as e:
                    logger.warning(f"Attempt {attempt+1} failed : {e}")
                    if attempt<retry_count-1:
                        time.sleep(5**(attempt+1))
                    else:
                        logger.error(f"failed to fetch batch number {i+1}")
        return all_data







