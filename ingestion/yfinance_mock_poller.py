import pandas as pd
import numpy as np
from datetime import datetime, timedelta
import random
from typing import Dict, List
from monitoring.logging_config import setup_logger

logger = setup_logger()

def generate_mock_yfinance_data(sector_data: Dict[str, List[str]], interval="5m"):
    """
    Generates mock data that mimics yfinance structure exactly
    Returns: dict of {symbol: DataFrame} with MultiIndex simulation
    """
    all_data = {}
    
    # Flatten symbols with sector mapping
    sector_map = {}
    all_symbols = []
    
    for sector, symbols in sector_data.items():
        for symbol in symbols:
            sector_map[symbol] = sector
            all_symbols.append(symbol)
    
    logger.info(f"Generating mock data for {len(all_symbols)} symbols")
    
    # Process in batches to mimic your logic
    batch_size = 8
    for i in range(0, len(all_symbols), batch_size):
        batch = all_symbols[i:i + batch_size]
        batch_num = i // batch_size + 1
        
        logger.info(f"Mock: Fetching batch {batch_num}")
        
        # Simulate yfinance download returning MultiIndex DataFrame
        mock_batch_data = {}
        
        for symbol in batch:
            # Generate realistic data based on symbol type
            df = _generate_symbol_mock_data(symbol, sector_map[symbol], interval)
            mock_batch_data[symbol] = df
            
            # Randomly simulate missing data (like yfinance does)
            if random.random() < 0.1:  # 10% chance of empty data
                logger.info(f"Mock: No data for {symbol} (simulating yfinance failure)")
                continue
        
        # Add batch data to result
        all_data.update(mock_batch_data)
        
        # Simulate API delay
        logger.info(f"Mock: Successfully fetched batch {batch_num} with {len(mock_batch_data)} symbols")
    
    logger.info(f"******************Mock data keys: {list(all_data.keys())}**********************")
    return all_data

def _generate_symbol_mock_data(symbol: str, sector: str, interval="5m"):
    """
    Generate realistic mock OHLCV data for a single symbol
    """
    # Determine base price based on symbol
    if 'BANK' in symbol or 'HDFC' in symbol or 'ICICI' in symbol:
        base_price = random.uniform(500, 2500)
        volatility = 0.02  # 2%
    elif 'RELIANCE' in symbol:
        base_price = random.uniform(2000, 3000)
        volatility = 0.015  # 1.5%
    elif 'TCS' in symbol or 'INFY' in symbol or 'WIPRO' in symbol:
        base_price = random.uniform(3000, 4000)
        volatility = 0.01  # 1%
    elif 'ITC' in symbol:
        base_price = random.uniform(200, 500)
        volatility = 0.01
    else:
        base_price = random.uniform(100, 1000)
        volatility = 0.03  # 3%
    
    # Determine number of intervals based on interval
    if interval == "1m":
        num_intervals = random.randint(200, 390)  # 1 day of 1-min data
    elif interval == "5m":
        num_intervals = random.randint(50, 78)  # 1 day of 5-min data
    elif interval == "15m":
        num_intervals = random.randint(20, 32)  # 1 day of 15-min data
    elif interval == "1h":
        num_intervals = random.randint(6, 9)  # 1 day of 1-hour data
    else:
        num_intervals = random.randint(6, 9)  # Default to hourly
    
    # Generate timestamps (going backwards from now)
    timestamps = []
    now = datetime.now().replace(second=0, microsecond=0)
    
    if interval == "1m":
        delta = timedelta(minutes=1)
    elif interval == "5m":
        delta = timedelta(minutes=5)
    elif interval == "15m":
        delta = timedelta(minutes=15)
    elif interval == "1h":
        delta = timedelta(hours=1)
    else:
        delta = timedelta(minutes=5)  # Default
    
    for j in range(num_intervals):
        ts = now - (delta * j)
        timestamps.append(ts)
    
    timestamps.reverse()  # Oldest to newest
    
    # Generate OHLCV data with realistic patterns
    opens = []
    highs = []
    lows = []
    closes = []
    volumes = []
    
    current_price = base_price
    
    for j in range(num_intervals):
        # Generate random walk with drift
        change = random.gauss(0, volatility)  # Normal distribution
        
        # Add some trend (upward bias for some symbols)
        if 'RELIANCE' in symbol or 'HDFCBANK' in symbol:
            change += 0.0005  # Slight upward bias
        
        new_price = current_price * (1 + change)
        
        # Calculate OHLC with realistic spreads
        open_price = current_price
        
        # Intraday movement (high and low)
        intraday_volatility = volatility * random.uniform(0.5, 1.5)
        high_price = max(open_price, new_price) * (1 + random.uniform(0, intraday_volatility))
        low_price = min(open_price, new_price) * (1 - random.uniform(0, intraday_volatility))
        
        # Ensure high >= low
        if high_price < low_price:
            high_price, low_price = low_price, high_price
        
        close_price = new_price
        
        # Add occasional spikes
        if random.random() < 0.05:  # 5% chance of spike
            if random.choice([True, False]):
                high_price *= 1.05  # 5% spike up
            else:
                low_price *= 0.95  # 5% spike down
        
        # Volume correlates with price movement
        volume = random.randint(10000, 1000000)
        price_change_pct = abs(close_price - open_price) / open_price
        volume *= (1 + price_change_pct * 10)  # More volume on bigger moves
        
        # Add NaN values randomly (simulating missing data)
        if random.random() < 0.05:  # 5% chance of NaN in any field
            nan_field = random.choice(['open', 'high', 'low', 'close', 'volume'])
            if nan_field == 'open':
                open_price = np.nan
            elif nan_field == 'high':
                high_price = np.nan
            elif nan_field == 'low':
                low_price = np.nan
            elif nan_field == 'close':
                close_price = np.nan
            elif nan_field == 'volume':
                volume = np.nan
        
        opens.append(round(open_price, 2) if not pd.isna(open_price) else np.nan)
        highs.append(round(high_price, 2) if not pd.isna(high_price) else np.nan)
        lows.append(round(low_price, 2) if not pd.isna(low_price) else np.nan)
        closes.append(round(close_price, 2) if not pd.isna(close_price) else np.nan)
        volumes.append(int(volume) if not pd.isna(volume) else np.nan)
        
        current_price = close_price
    
    # Create DataFrame
    df = pd.DataFrame({
        'Open': opens,
        'High': highs,
        'Low': lows,
        'Close': closes,
        'Volume': volumes
    }, index=timestamps)
    
    # Add sector column
    df['sector'] = sector
    
    # Add some missing sectors for some symbols (simulating real data issues)
    if random.random() < 0.1:  # 10% chance of missing sector
        df['sector'] = np.nan
    
    return df

def _generate_multiindex_mock_data(batch: List[str], sector_map: Dict[str, str], interval="5m"):
    """
    Generate a MultiIndex DataFrame to simulate yfinance's group_by='ticker'
    """
    data_frames = {}
    
    for symbol in batch:
        df = _generate_symbol_mock_data(symbol, sector_map.get(symbol, "UNKNOWN"), interval)
        data_frames[symbol] = df
    
    # Create MultiIndex DataFrame (simulating yfinance's output)
    panel_data = {}
    for symbol, df in data_frames.items():
        for column in ['Open', 'High', 'Low', 'Close', 'Volume']:
            panel_data[(symbol, column)] = df[column]
    
    # Create MultiIndex columns
    multi_index = pd.MultiIndex.from_tuples(panel_data.keys(), names=['Symbol', 'Attribute'])
    
    # Create DataFrame
    result_df = pd.DataFrame(panel_data, index=df.index)
    result_df.columns = multi_index
    
    return result_df



