package com.cs.rfq.decorator;

import org.apache.spark.SparkConf;
import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.SparkSession;
import org.apache.spark.streaming.Durations;
import org.apache.spark.streaming.api.java.JavaDStream;
import org.apache.spark.streaming.api.java.JavaStreamingContext;

import java.util.Arrays;

public class RfqDecoratorMain {

    public static void main(String[] args) throws Exception {
        System.setProperty("hadoop.home.dir", "C:\\Java\\hadoop-2.9.2");
        System.setProperty("spark.master", "local[4]");

        // Spark configuration
        SparkConf conf = new SparkConf().setAppName("SparklingTradersApp");

        // Spark streaming context
        JavaStreamingContext jssc = new JavaStreamingContext(conf, Durations.seconds(1));

        // Spark session
        SparkSession spark = SparkSession.builder().config(conf).getOrCreate();

        // create a new RfqProcessor and set it listening for incoming RFQs
        RfqProcessor proc = new RfqProcessor(spark, jssc);
        proc.startSocketListener();
    }

}
