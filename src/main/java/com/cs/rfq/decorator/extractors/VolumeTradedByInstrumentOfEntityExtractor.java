package com.cs.rfq.decorator.extractors;

import com.cs.rfq.decorator.Rfq;
import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.SparkSession;
import org.joda.time.DateTime;

import java.util.HashMap;
import java.util.Map;

public class VolumeTradedByInstrumentOfEntityExtractor implements RfqMetadataExtractor  {
    private String since;

    public VolumeTradedByInstrumentOfEntityExtractor() {
        //this.since = DateTime.now().getYear() + "-01-01";
        this.since = "2019-06-01";
    }


    @Override
    public Map<RfqMetadataFieldNames, Object> extractMetaData(Rfq rfq, SparkSession session, Dataset<Row> trades) {

        String queryYTD = String.format("SELECT sum(LastQty) from trade where EntityId='%s' AND SecurityId='%s' AND TradeDate >= '%s'",
                rfq.getEntityId(),
                rfq.getIsin(),
                since);

        trades.createOrReplaceTempView("trade");
        Dataset<Row> sqlQueryResultsYTD = session.sql(queryYTD);

        Object volumeYTD = sqlQueryResultsYTD.first().get(0);
        if (volumeYTD == null) {
            volumeYTD = 0L;
        }

        //this.setSince(DateTime.now().getYear() + "-" +DateTime.now().getMonthOfYear() + "-01");
        this.setSince("2019-05-01");
        String queryMTD = String.format("SELECT sum(LastQty) from trade where EntityId='%s' AND SecurityId='%s' AND TradeDate >= '%s'",
                rfq.getEntityId(),
                rfq.getIsin(),
                since);

        trades.createOrReplaceTempView("trade");
        Dataset<Row> sqlQueryResultsMTD = session.sql(queryMTD);

        Object volumeMTD = sqlQueryResultsMTD.first().get(0);
        if (volumeMTD == null) {
            volumeMTD = 0L;
        }

        //this.setSince(DateTime.now().getYear() + "-" +DateTime.now().getMonthOfYear() + "-" +
        this.setSince("2019-06-07");
        String queryWTD = String.format("SELECT sum(LastQty) from trade where EntityId='%s' AND SecurityId='%s' AND TradeDate >= '%s'",
                rfq.getEntityId(),
                rfq.getIsin(),
                since);

        trades.createOrReplaceTempView("trade");
        Dataset<Row> sqlQueryResultsWTD = session.sql(queryWTD);

        Object volumeWTD = sqlQueryResultsWTD.first().get(0);
        if (volumeWTD == null) {
            volumeWTD = 0L;
        }

        Map<RfqMetadataFieldNames, Object> results = new HashMap<>();
        results.put(RfqMetadataFieldNames.volumeOfInstrumentWithEntityPastWeek, volumeWTD);
        results.put(RfqMetadataFieldNames.volumeOfInstrumentWithEntityPastMonth, volumeMTD);
        results.put(RfqMetadataFieldNames.volumeOfInstrumentWithEntityPastYear, volumeYTD);
        return results;
    }

    protected void setSince(String since) {
        this.since = since;
    }
}
