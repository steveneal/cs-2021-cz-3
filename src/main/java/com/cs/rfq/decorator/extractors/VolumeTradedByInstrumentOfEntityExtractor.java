package com.cs.rfq.decorator.extractors;

import com.cs.rfq.decorator.Rfq;
import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.SparkSession;
import org.joda.time.DateTime;

import java.util.HashMap;
import java.util.Map;

public class VolumeTradedByInstrumentOfEntityExtractor implements RfqMetadataExtractor  {
    public DateTime since;

    public VolumeTradedByInstrumentOfEntityExtractor() {
        since = DateTime.now();
    }



    @Override
    public Map<RfqMetadataFieldNames, Object> extractMetaData(Rfq rfq, SparkSession session, Dataset<Row> trades) {

        long todayMs = since.withMillisOfDay(0).getMillis();
        long pastMonthMs = since.withMillis(todayMs).minusMonths(1).getMillis();
        long pastMonthYs = since.withMillis(todayMs).minusYears(1).getMillis();
        long pastMonthWs = since.withMillis(todayMs).minusWeeks(1).getMillis();

        Dataset<Row> filteredYs = trades
                .filter(trades.col("SecurityId").equalTo(rfq.getIsin()))
                .filter(trades.col("EntityId").equalTo(rfq.getEntityId()))
                .filter(trades.col("TradeDate").$greater(new java.sql.Date(pastMonthYs)))
                .filter(trades.col("TradeDate").$less(new java.sql.Date(todayMs)));

        filteredYs.createOrReplaceTempView("trade");
        Dataset<Row> sqlQueryResultsYs = session.sql("SELECT SUM(LastQty) FROM trade");

        Object totVolumeYs = sqlQueryResultsYs.first().get(0);
        if (totVolumeYs == null)
            totVolumeYs = 0L;



        Dataset<Row> filteredMs = trades
                .filter(trades.col("SecurityId").equalTo(rfq.getIsin()))
                .filter(trades.col("EntityId").equalTo(rfq.getEntityId()))
                .filter(trades.col("TradeDate").$greater(new java.sql.Date(pastMonthMs)))
                .filter(trades.col("TradeDate").$less(new java.sql.Date(todayMs)));

        filteredMs.createOrReplaceTempView("trade");
        Dataset<Row> sqlQueryResultsMs = session.sql("SELECT SUM(LastQty) FROM trade");

        Object totVolumeMs = sqlQueryResultsMs.first().get(0);
        if (totVolumeMs == null)
            totVolumeMs = 0L;


        Dataset<Row> filteredWs = trades
                .filter(trades.col("SecurityId").equalTo(rfq.getIsin()))
                .filter(trades.col("EntityId").equalTo(rfq.getEntityId()))
                .filter(trades.col("TradeDate").$greater(new java.sql.Date(pastMonthWs)))
                .filter(trades.col("TradeDate").$less(new java.sql.Date(todayMs)));

        filteredWs.createOrReplaceTempView("trade");
        Dataset<Row> sqlQueryResultsWs = session.sql("SELECT SUM(LastQty) FROM trade");

        Object totVolumeWs = sqlQueryResultsWs.first().get(0);
        if (totVolumeWs == null)
            totVolumeWs = 0L;



        Map<RfqMetadataFieldNames, Object> results = new HashMap<>();
        results.put(RfqMetadataFieldNames.volumeOfInstrumentWithEntityPastWeek, totVolumeWs);
        results.put(RfqMetadataFieldNames.volumeOfInstrumentWithEntityPastMonth, totVolumeMs);
        results.put(RfqMetadataFieldNames.volumeOfInstrumentWithEntityPastYear, totVolumeYs);
        return results;
    }

    protected void setSince(String date) {
        this.since = DateTime.parse(date);
    }
}
