package com.cs.rfq.decorator;

import org.apache.spark.SparkContext;
import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.SparkSession;
import org.apache.spark.sql.types.*;
import org.apache.spark.streaming.api.java.JavaDStream;
import org.mortbay.log.Log;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import static org.apache.spark.sql.types.DataTypes.*;

public class TradeDataLoader {

    private final static Logger log = LoggerFactory.getLogger(TradeDataLoader.class);

    public Dataset<Row> loadTrades(SparkSession session, String path) {
        // create an explicit schema for the trade data in the JSON files
        StructType schema = new StructType(new StructField[] {
                new StructField("TraderId", LongType, false, Metadata.empty()),
                new StructField("EntityId", LongType, false, Metadata.empty()),
                new StructField("MsgType", IntegerType, false, Metadata.empty()),
                new StructField("TradeReportId", LongType, false, Metadata.empty()),
                new StructField("PreviouslyReported", StringType, false, Metadata.empty()),
                new StructField("SecurityID", StringType, false, Metadata.empty()),
                new StructField("SecurityIdSource", IntegerType, false, Metadata.empty()),
                new StructField("LastQty", LongType, false, Metadata.empty()),
                new StructField("LastPx", DoubleType, false, Metadata.empty()),
                new StructField("TradeDate", DateType, false, Metadata.empty()),
                new StructField("TransactTime", StringType, false, Metadata.empty()),
                new StructField("NoSides", IntegerType, false, Metadata.empty()),
                new StructField("Side", IntegerType, false, Metadata.empty()),
                new StructField("OrderID", LongType, false, Metadata.empty()),
                new StructField("Currency", StringType, false, Metadata.empty())
        });

        // load the trades dataset
        Dataset<Row> trades = session.read().schema(schema).json(path);

        // log a message indicating number of records loaded and the schema used
        trades.printSchema();

        Log.info("Number of records loaded. " + trades.count());

        return trades;
    }


}
