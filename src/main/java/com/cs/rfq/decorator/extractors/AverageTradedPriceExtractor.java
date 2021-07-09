package com.cs.rfq.decorator.extractors;

import com.cs.rfq.decorator.Rfq;
import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.SparkSession;
import org.joda.time.DateTime;

import java.util.HashMap;
import java.util.Map;

public class AverageTradedPriceExtractor implements RfqMetadataExtractor{

    public DateTime since;

    public AverageTradedPriceExtractor() {
        since = DateTime.now();
    }

    @Override
    public Map<RfqMetadataFieldNames, Object> extractMetaData(Rfq rfq, SparkSession session, Dataset<Row> trades) {

        //this.setSince("2019-06-10");
        long todayMs = since.withMillisOfDay(0).getMillis();
        long pastWeekMs = since.withMillis(todayMs).minusWeeks(1).getMillis();


        Dataset<Row> filteredWs = trades
                .filter(trades.col("SecurityId").equalTo(rfq.getIsin()))
                .filter(trades.col("EntityId").equalTo(rfq.getEntityId()))
                .filter(trades.col("TradeDate").$greater(new java.sql.Date(pastWeekMs)))
                .filter(trades.col("TradeDate").$less(new java.sql.Date(todayMs)));

        filteredWs.createOrReplaceTempView("trade");
        Dataset<Row> sqlQueryResultsWs = session.sql("SELECT AVG(LastPx) FROM trade");

        Object avgPriceWs = sqlQueryResultsWs.first().get(0);
        if (avgPriceWs == null)
            avgPriceWs = 0L;


        Map<RfqMetadataFieldNames, Object> results = new HashMap<>();
        results.put(RfqMetadataFieldNames.averageTradedPricePastWeek, avgPriceWs);
        return results;
    }

    protected void setSince(String date) {
        this.since = DateTime.parse(date);
    }
}
