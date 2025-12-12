import json
import time
from kafka import KafkaProducer
import schedule
from datetime import datetime
from monitoring.logging_config import setup_logger
from .yfinance_mock_poller import generate_mock_yfinance_data as fetch_stock_data
from .nifty_symbol import sector_portfolios
import os
logger=setup_logger()

class StockDataProducer:
    def __init__(self):
        bootstrap_servers=os.getenv("KAFKA_BOOTSTRAP_SERVERS","kafka:9092")
        logger.info(f"Attempting to connect to Kafka at: {bootstrap_servers}")
        self.producer=KafkaProducer(
            bootstrap_servers=bootstrap_servers,
            value_serializer=lambda v:json.dumps(v).encode('utf-8'),
            acks='all',
            retries=3
        )
    def process_and_send_to_kafka(self,stock_data_dict):
        messages_sent=0
        for symbol,df in stock_data_dict.items():
            sector=df['sector'].iloc[0] if 'sector' in df.columns else "UNKNOWN"
            for index,row in df.iterrows():
                message = {
                    "symbol": symbol,
                    "timestamp": index.isoformat(),
                    "open": row['Open'],
                    "high": row['High'],
                    "low": row['Low'],
                    "close": row['Close'],
                    "volume": row['Volume'],
                    "sector": sector,
                    "source": "yfinance",
                    "ingestion_time": datetime.utcnow().isoformat()
                }
                self.producer.send("raw-stock",value=message)
                messages_sent+=1
        
        self.producer.flush()
        logger.info(f"Sent {messages_sent} messages to Kafka")
        return messages_sent
    def run_fetch_and_send(self):
        logger.info("Starting data fetch cycle...")
        stock_data=fetch_stock_data(sector_portfolios)
        if stock_data:
            logger.info(f"stock_data: {stock_data}")
            count=self.process_and_send_to_kafka(stock_data_dict=stock_data)
            logger.info(f"Cycle complete. Processed {len(stock_data)} symbols, {count} records")
        else:
            logger.warning("No data fetched in this cycle")   
        return stock_data
    
def main():
    producer = StockDataProducer()
    schedule.every(5).minutes.do(producer.run_fetch_and_send)
    logger.info("Stock Data Producer started. Scheduling runs every 5 minutes...")
    producer.run_fetch_and_send()  
    while True:  
        schedule.run_pending()  
        time.sleep(1) 

if __name__ == "__main__":
    main()