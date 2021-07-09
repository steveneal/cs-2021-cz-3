package com.cs.rfq.decorator.extractors;

import com.cs.rfq.decorator.Rfq;
import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.SparkSession;
import org.joda.time.DateTime;

import java.util.HashMap;
import java.util.Map;

import static com.cs.rfq.decorator.extractors.RfqMetadataFieldNames.*;

public class TradeSideBiasWithEntityExtractor implements RfqMetadataExtractor {

    /*
    Our component should report the ratio (as a single figure) of buy/sell traded for downstream systems to display.
    Figures for weekly and monthly trading should be sent. A negative number will indicate that there was no trading for this instrument in the given period.
     */

    private DateTime since;

    public TradeSideBiasWithEntityExtractor() {
        since = DateTime.now();
    }

    public TradeSideBiasWithEntityExtractor(String date) {
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

        Dataset<Row> filtered = trades.filter(trades.col("EntityId").equalTo(rfq.getEntityId()));

        Map<RfqMetadataFieldNames, Object> results = new HashMap<>();
        results.put(tradeRatioWithEntityPastWeek, computeRatio(filtered, session, trades, pastWeekMs, todayMs));
        results.put(tradeRatioWithEntityPastMonth, computeRatio(filtered, session, trades, pastMonthMs, todayMs));
        return results;
    }

    private double computeRatio(Dataset<Row> filtered, SparkSession session, Dataset<Row> trades, long lowerBoundMs, long upperBoundMs) {
        Dataset<Row> pastTrades = filtered.filter(trades.col("TradeDate").$greater(new java.sql.Date(lowerBoundMs))).filter(trades.col("TradeDate").$less(new java.sql.Date(upperBoundMs)));
        pastTrades.createOrReplaceTempView("trade");
        Dataset<Row> sqlQueryResultsBW = session.sql("SELECT SUM(NoSides) FROM trade WHERE Side=1");
        Object numB = sqlQueryResultsBW.first().get(0);
        double nb = 0.0;
        if (numB != null)
            nb = ((java.lang.Long) numB).doubleValue();

        Dataset<Row> sqlQueryResultsSW = session.sql("SELECT SUM(NoSides) FROM trade WHERE Side=2");
        Object numS = sqlQueryResultsSW.first().get(0);
        double ns = 0.0;
        if (numS != null)
            ns = ((java.lang.Long) numS).doubleValue();

        double ratio = 0;
        if (nb == 0.0 && ns == 0.0)
            ratio = -1;
        else {
            if (nb != 0.0)
                if (ns != 0.0)
                    ratio = nb/ns;
                else
                    ratio = -3;
            else
                ratio = -2;
        }

        return ratio;
    }

}
