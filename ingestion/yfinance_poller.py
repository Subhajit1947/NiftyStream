import yfinance as yf
import pandas as pd
import time
from monitoring.logging_config import setup_logger

logger = setup_logger()

def fetch_stock_data(sector_data, interval="5m", retry_count=3):
    all_data = {}

    for sector, symbols in sector_data.items():
        batch_size = 8
        for i in range(0, len(symbols), batch_size):
            batch = symbols[i:i + batch_size]

            for attempt in range(retry_count):
                try:
                    logger.info(f"try to fetch batch number {i + 1}")
                    data = yf.download(
                        tickers=batch,
                        period="1d",
                        interval=interval,
                        group_by="ticker",
                        progress=False,
                        threads=False,
                        proxy=None,
                    )

                    if data.empty:
                        logger.warning("Empty dataframe for batch %s", batch)
                        break

                    # Log what actually came back
                    try:
                        cols_level0 = (
                            data.columns.get_level_values(0)
                            if hasattr(data.columns, "levels")
                            else data.columns
                        )
                        logger.info("data received from api: %s", cols_level0)
                    except Exception as e:
                        logger.warning("Could not inspect columns: %s", e)

                    for sym in batch:
                        # Multi-ticker case: columns are MultiIndex, first level is ticker
                        if hasattr(data.columns, "levels"):
                            tickers_in_df = data.columns.get_level_values(0)
                            logger.info("sym %s in columns: %s", sym, sym in tickers_in_df)
                            if sym in tickers_in_df:
                                df = data[sym].copy()
                                df["sector"] = sector
                                all_data[sym] = df

                        # Single-ticker fallback (no MultiIndex)
                        else:
                            logger.info("single-ticker / flat columns for %s", sym)
                            df = data.copy()
                            df["sector"] = sector
                            all_data[sym] = df

                    logger.info(
                        "Successfully fetched batch number %s with %d symbols",
                        i + 1,
                        len(batch),
                    )
                    time.sleep(2)
                    break

                except Exception as e:
                    logger.warning("Attempt %d failed: %s", attempt + 1, e)
                    if attempt < retry_count - 1:
                        time.sleep(5 ** (attempt + 1))
                    else:
                        logger.error("failed to fetch batch number %s", i + 1)

    logger.info("******************all data keys: %s**********************", list(all_data.keys()))
    return all_data
