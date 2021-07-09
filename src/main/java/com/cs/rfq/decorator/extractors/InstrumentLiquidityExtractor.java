package com.cs.rfq.decorator.extractors;

import com.cs.rfq.decorator.Rfq;
import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.SparkSession;
import org.joda.time.DateTime;

import java.util.HashMap;
import java.util.Map;

import static com.cs.rfq.decorator.extractors.RfqMetadataFieldNames.*;

public class InstrumentLiquidityExtractor implements RfqMetadataExtractor {

    private DateTime since;

    public InstrumentLiquidityExtractor() {
        since = DateTime.now();
    }

    public InstrumentLiquidityExtractor(String date) {
        since = DateTime.parse(date);
    }

    @Override
    public Map<RfqMetadataFieldNames, Object> extractMetaData(Rfq rfq, SparkSession session, Dataset<Row> trades) {
        long todayMs = since.withMillisOfDay(0).getMillis();
        long pastMonthMs = since.withMillis(todayMs).minusMonths(1).getMillis();

        Dataset<Row> filtered = trades
                .filter(trades.col("SecurityId").equalTo(rfq.getIsin()))
                .filter(trades.col("TradeDate").$greater(new java.sql.Date(pastMonthMs)))
                .filter(trades.col("TradeDate").$less(new java.sql.Date(todayMs)));

        filtered.createOrReplaceTempView("trade");
        Dataset<Row> sqlQueryResults = session.sql("SELECT SUM(LastQty) FROM trade");

        Object totVolume = sqlQueryResults.first().get(0);
        if (totVolume == null)
            totVolume = 0L;

        Map<RfqMetadataFieldNames, Object> results = new HashMap<>();
        results.put(instrumentLiquidity, totVolume);
        return results;
    }

    public void setSince(String date) {
        since = DateTime.parse(date);
    }

}
