package com.cs.rfq.decorator.extractors;

import com.cs.rfq.decorator.Rfq;
import com.cs.rfq.decorator.TradeDataLoader;
import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Row;
import org.junit.Before;
import org.junit.Test;

import java.util.Map;

import static org.junit.Assert.assertEquals;

public class TradeSideBiasWithEntityExtractorTest extends AbstractSparkUnitTest {

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

        TradeSideBiasWithEntityExtractor extractor = new TradeSideBiasWithEntityExtractor();
        extractor.setSince("2018-06-10");

        Map<RfqMetadataFieldNames, Object> meta = extractor.extractMetaData(rfq, session, trades);

        Object resultW = meta.get(RfqMetadataFieldNames.tradeRatioWithEntityPastWeek);
        assertEquals(1.0, resultW);

        Object resultM = meta.get(RfqMetadataFieldNames.tradeRatioWithEntityPastMonth);
        assertEquals(0.5, resultM);
    }

    @Test
    public void checkVolumeWhenNoTradesMatch() {
        String filePath = getClass().getResource("volume-traded-1.json").getPath();
        Dataset<Row> trades = new TradeDataLoader().loadTrades(session, filePath);

        TradeSideBiasWithEntityExtractor extractor = new TradeSideBiasWithEntityExtractor();

        Map<RfqMetadataFieldNames, Object> meta = extractor.extractMetaData(rfq, session, trades);

        Object resultW = meta.get(RfqMetadataFieldNames.tradeRatioWithEntityPastWeek);
        assertEquals(-1.0, resultW);
    }

    @Test
    public void checkVolumeWhenOnlyBuyTradesWereMade() {
        String filePath = getClass().getResource("volume-traded-1.json").getPath();
        Dataset<Row> trades = new TradeDataLoader().loadTrades(session, filePath);

        TradeSideBiasWithEntityExtractor extractor = new TradeSideBiasWithEntityExtractor();
        extractor.setSince("2017-07-11");

        Map<RfqMetadataFieldNames, Object> meta = extractor.extractMetaData(rfq, session, trades);

        Object resultW = meta.get(RfqMetadataFieldNames.tradeRatioWithEntityPastWeek);
        assertEquals(-3.0, resultW);
    }

    @Test
    public void checkVolumeWhenOnlySellTradesWereMade() {
        String filePath = getClass().getResource("volume-traded-1.json").getPath();
        Dataset<Row> trades = new TradeDataLoader().loadTrades(session, filePath);

        TradeSideBiasWithEntityExtractor extractor = new TradeSideBiasWithEntityExtractor();
        extractor.setSince("2017-08-11");

        Map<RfqMetadataFieldNames, Object> meta = extractor.extractMetaData(rfq, session, trades);

        Object resultW = meta.get(RfqMetadataFieldNames.tradeRatioWithEntityPastWeek);
        assertEquals(-2.0, resultW);
    }


}
