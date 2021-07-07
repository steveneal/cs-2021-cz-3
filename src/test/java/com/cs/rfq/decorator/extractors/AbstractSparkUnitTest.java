package com.cs.rfq.decorator.extractors;

import org.apache.spark.SparkConf;
import org.apache.spark.sql.SparkSession;
import org.junit.AfterClass;
import org.junit.BeforeClass;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class AbstractSparkUnitTest {

    private final static Logger log = LoggerFactory.getLogger(AbstractSparkUnitTest.class);

    protected static SparkSession session;

    @BeforeClass
    public static void setupClass() {
        System.setProperty("hadoop.home.dir", "C:\\Java\\hadoop-2.9.2");

        SparkConf conf = new SparkConf()
                .setMaster("local[*]")
                .setAppName("SparkUnitTest");

        session = SparkSession.builder().config(conf).getOrCreate();

        log.info("Spark setup complete");
    }

    @AfterClass
    public static void teardownClass() {
        session.stop();
        log.info("Spark teardown complete");
    }

}
