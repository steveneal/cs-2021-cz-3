package com.cs.rfq.decorator.extractors;

import com.cs.rfq.decorator.Rfq;
import com.cs.rfq.decorator.TradeDataLoader;
import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Row;
import org.junit.Before;
import org.junit.Test;

import java.util.Map;

import static org.junit.Assert.assertEquals;

public class TotalVolTradedWithEntityExtractorTest extends AbstractSparkUnitTest {

    private Rfq rfq;

    @Before
    public void setup() {
        rfq = new Rfq();
        rfq.setEntityId(5561279226039690843L);
        rfq.setIsin("AT0000A0VRQ6");
    }

    @Test
    public void checkVolumeWhenAllTradesMatch() {

        String filePath = getClass().getResource("volume-traded-1.json").getPath();
        Dataset<Row> trades = new TradeDataLoader().loadTrades(session, filePath);

        TotalVolTradedWithEntityExtractor extractor = new TotalVolTradedWithEntityExtractor();
        extractor.setSince("2018-06-10");

        Map<RfqMetadataFieldNames, Object> meta = extractor.extractMetaData(rfq, session, trades);

        Object resultW = meta.get(RfqMetadataFieldNames.volumeWithEntityPastWeek);
        assertEquals(550000L, resultW);

        Object resultM = meta.get(RfqMetadataFieldNames.volumeWithEntityPastMonth);
        assertEquals(600000L, resultM);

        Object resultY = meta.get(RfqMetadataFieldNames.volumeWithEntityPastYear);
        assertEquals(1350000L, resultY);
    }

    @Test
    public void checkVolumeWhenNoTradesMatch() {

        String filePath = getClass().getResource("volume-traded-1.json").getPath();
        Dataset<Row> trades = new TradeDataLoader().loadTrades(session, filePath);

        TotalVolTradedWithEntityExtractor extractor = new TotalVolTradedWithEntityExtractor();


        Map<RfqMetadataFieldNames, Object> meta = extractor.extractMetaData(rfq, session, trades);

        Object resultW = meta.get(RfqMetadataFieldNames.volumeWithEntityPastWeek);
        assertEquals(0L, resultW);

        Object resultM = meta.get(RfqMetadataFieldNames.volumeWithEntityPastMonth);
        assertEquals(0L, resultM);

        Object resultY = meta.get(RfqMetadataFieldNames.volumeWithEntityPastYear);
        assertEquals(0L, resultY);
    }


}
