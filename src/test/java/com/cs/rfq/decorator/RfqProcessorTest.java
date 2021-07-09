package com.cs.rfq.decorator;

import com.cs.rfq.utils.ChatterboxServer;
import junit.framework.AssertionFailedError;
import org.apache.spark.SparkConf;
import org.apache.spark.sql.SparkSession;
import org.apache.spark.streaming.Durations;
import org.apache.spark.streaming.api.java.JavaStreamingContext;
import org.junit.Before;
import org.junit.Test;


public class RfqProcessorTest {

    JavaStreamingContext jssc;
    SparkSession spark;

    @Before
    public void setup() {
        System.setProperty("hadoop.home.dir", "C:\\Java\\hadoop-2.9.2");
        System.setProperty("spark.master", "local[4]");

        SparkConf conf = new SparkConf().setAppName("SparklingTradersApp");
        jssc = new JavaStreamingContext(conf, Durations.seconds(1));
        spark = SparkSession.builder().config(conf).getOrCreate();
    }

    @Test(expected = NullPointerException.class)
    public void testStartSocketListenerPass() throws InterruptedException {
        RfqProcessor proc = new RfqProcessor(spark, jssc);
        proc.startSocketListener();
    }

}
