package com.cs.rfq.decorator.extractors;

import com.cs.rfq.decorator.Rfq;
import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.SparkSession;
import org.joda.time.DateTime;

import java.util.HashMap;
import java.util.Map;

import static com.cs.rfq.decorator.extractors.RfqMetadataFieldNames.*;

public class TotalVolTradedWithEntityExtractor implements RfqMetadataExtractor {
    private DateTime since;

    public TotalVolTradedWithEntityExtractor() {
        since = DateTime.now();
    }

    public TotalVolTradedWithEntityExtractor(String date) {
        since = DateTime.parse(date);
    }
    public void setSince(String date) {
        since = DateTime.parse(date);
    }
    @Override
    public Map<RfqMetadataFieldNames, Object> extractMetaData(Rfq rfq, SparkSession session, Dataset<Row> trades) {

        long todayMs = since.withMillisOfDay(0).getMillis();
        long pastWeekMs = since.withMillis(todayMs).minusWeeks(1).getMillis();
        long pastMonthMs = since.withMillis(todayMs).minusMonths(1).getMillis();
        long pastYearMs = since.withMillis(todayMs).minusYears(1).getMillis();

        Dataset<Row> filtered = trades
                .filter(trades.col("EntityId").equalTo(rfq.getEntityId()));

        Dataset<Row> tradesPastWeek = filtered.filter(trades.col("TradeDate").$greater(new java.sql.Date(pastWeekMs))).filter(trades.col("TradeDate").$less(new java.sql.Date(todayMs)));
        tradesPastWeek.createOrReplaceTempView("trade");
        Dataset<Row> sqlQueryResultsW = session.sql("SELECT SUM(LastQty) FROM trade");
        Object totVolumePastWeek = sqlQueryResultsW.first().get(0);
        if (totVolumePastWeek == null)
            totVolumePastWeek = 0L;

        Dataset<Row> tradesPastMonth = filtered.filter(trades.col("TradeDate").$greater(new java.sql.Date(pastMonthMs))).filter(trades.col("TradeDate").$less(new java.sql.Date(todayMs)));
        tradesPastMonth.createOrReplaceTempView("trade");
        Dataset<Row> sqlQueryResultsM = session.sql("SELECT SUM(LastQty) FROM trade");
        Object totVolumePastMonth = sqlQueryResultsM.first().get(0);
        if (totVolumePastMonth == null)
            totVolumePastMonth = 0L;

        Dataset<Row> tradesPastYear = filtered.filter(trades.col("TradeDate").$greater(new java.sql.Date(pastYearMs))).filter(trades.col("TradeDate").$less(new java.sql.Date(todayMs)));
        tradesPastYear.createOrReplaceTempView("trade");
        Dataset<Row> sqlQueryResultsY = session.sql("SELECT SUM(LastQty) FROM trade");
        Object totVolumePastYear = sqlQueryResultsY.first().get(0);
        if (totVolumePastYear == null)
            totVolumePastYear = 0L;

        Map<RfqMetadataFieldNames, Object> results = new HashMap<>();
        results.put(volumeWithEntityPastMonth, totVolumePastMonth);
        results.put(volumeWithEntityPastWeek, totVolumePastWeek);
        results.put(volumeWithEntityPastYear, totVolumePastYear);
        return results;
    }

}
