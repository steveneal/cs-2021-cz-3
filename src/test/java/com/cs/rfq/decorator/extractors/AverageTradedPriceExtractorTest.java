package com.cs.rfq.decorator.extractors;

import com.cs.rfq.decorator.Rfq;
import com.cs.rfq.decorator.TradeDataLoader;
import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Row;
import org.junit.Before;
import org.junit.Test;

import java.util.Map;

import static org.junit.Assert.assertEquals;

public class AverageTradedPriceExtractorTest extends AbstractSparkUnitTest {

    private Rfq rfq;


    @Before
    public void setup() {
        rfq = new Rfq();
        rfq.setEntityId(5561279226039690843L);
        rfq.setIsin("AT0000A0VRQ6");
    }

    @Test
    public void checkPriceWhenAllTradesMatchWTD() {

        String filePath = getClass().getResource("volume-traded-3.json").getPath();
        Dataset<Row> trades = new TradeDataLoader().loadTrades(session, filePath);

        AverageTradedPriceExtractor extractor = new AverageTradedPriceExtractor();
        extractor.setSince("2018-06-15");

        Map<RfqMetadataFieldNames, Object> meta = extractor.extractMetaData(rfq, session, trades);

        Object result = meta.get(RfqMetadataFieldNames.averageTradedPricePastWeek);
        assertEquals(127.2765, result);
    }

    @Test
    public void checkPriceWhenNoTradesMatchWTD() {

        String filePath = getClass().getResource("volume-traded-3.json").getPath();
        Dataset<Row> trades = new TradeDataLoader().loadTrades(session, filePath);

        //all test trade data are for 2018 so this will cause no matches
        AverageTradedPriceExtractor extractor = new AverageTradedPriceExtractor();
        extractor.setSince("2019-06-07");

        Map<RfqMetadataFieldNames, Object> meta = extractor.extractMetaData(rfq, session, trades);

        Object result = meta.get(RfqMetadataFieldNames.averageTradedPricePastWeek);

        assertEquals(0L, result);
    }
}
