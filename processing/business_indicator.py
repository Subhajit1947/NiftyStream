
from pyspark.sql.functions import *
from pyspark.sql.window import Window


def claculate_business_indicator(df):
    window_5min=Window.partitionBy("symbol").orderBy("timestamp").rowsBetween(-4,0)
    window_20min=Window.partitionBy("symbol").orderBy("timestamp").rowsBetween(-19,0)
    df=df.withColumn("price_change",col("close")-col("open"))\
        .withColumn("percent_change",round((col("price_change")/col("open"))*100,2))
    
    #calculate moving averages
    df=df.withColumn("sma_5",round(avg("close").over(window_5min),2))\
        .withColumn("sma_20",round(avg("close").over(window_20min),2))
    
    #claculate volume metrics
    df=df.withColumn("avg_volume_20",round(avg("volume").over(window_20min)))\
        .withColumn("volume_ratio",round(col("volume")/col("avg_volume_20"),2))\
        .withColumn("volume_surge",when(col("volume_ratio")>3,True).otherwise(False))
    #calculate price spike(5min change >2%)
    df=df.withColumn("prev_close",lag("close",1).over(Window.partitionBy("symbol").orderBy("timestamp")))\
        .withColumn("minute_percent_change",round((col("close")-col("prev_close"))/col("prev_close")*100,2))\
        .withColumn("price_spike",when(col("minute_percent_change")>2,True).otherwise(False))
    #calculate RSI
    price_change=when(col("close")>col("prev_close"),col("close")-col("prev_close")).otherwise(0)
    price_change_neg=when(col("close")<col("prev_close"),abs(col("close") - col("prev_close"))).otherwise(0)
    df=df.withColumn("gain",price_change)\
        .withColumn("loss",price_change_neg)
    
    #calculate average gain/loss over 14 period
    window_14=Window.partitionBy("symbol").orderBy("timestamp").rowsBetween(-13,0)
    df=df.withColumn("avg_gain",round(avg(col("gain")).over(window_14),2))\
        .withColumn("avg_loss",round(avg("loss").over(window_14),2))\
        .withColumn("rs",when(col("avg_loss")==0,100).otherwise(round(col("avg_gain")/col("avg_loss"),2)))\
        .withColumn("rsi",round(100-(100/(1+col("rs"))),2))\
        .withColumn("rsi_oversold",when(col("rsi")<30,True).otherwise(False))\
        .withColumn("rsi_overbought",when(col("rsi")>70,True).otherwise(False))
    
    #calculate vwap(for the day)
    window_vwap=Window.partitionBy("symbol",to_date("timestamp")).orderBy("timestamp")
    df.withColumn("cumulative_volume",sum("volume").over(window_vwap))\
        .withColumn("cumulative_price_volume",sum(col("close")*col("volume")).over(window_vwap))\
        .withColumn("vwap",round(col("cumulative_price_volume")/col("cumulative_volume"),2))
    return df
        








