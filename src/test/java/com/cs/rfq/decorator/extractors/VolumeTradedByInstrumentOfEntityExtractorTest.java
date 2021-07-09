package com.cs.rfq.decorator.extractors;

import com.cs.rfq.decorator.Rfq;
import com.cs.rfq.decorator.TradeDataLoader;
import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Row;
import org.junit.Before;
import org.junit.Test;

import java.util.Map;

import static org.junit.Assert.assertEquals;

public class VolumeTradedByInstrumentOfEntityExtractorTest extends AbstractSparkUnitTest {

    private Rfq rfq;


    @Before
    public void setup() {
        rfq = new Rfq();
        rfq.setEntityId(5561279226039690843L);
        rfq.setIsin("AT0000A0VRQ6");
    }

    @Test
    public void checkVolumeWhenAllTradesMatchYTD() {

        String filePath = getClass().getResource("volume-traded-1.json").getPath();
        Dataset<Row> trades = new TradeDataLoader().loadTrades(session, filePath);

        VolumeTradedByInstrumentOfEntityExtractor extractor = new VolumeTradedByInstrumentOfEntityExtractor();
        extractor.setSince("2018-06-11");

        Map<RfqMetadataFieldNames, Object> meta = extractor.extractMetaData(rfq, session, trades);

        Object result = meta.get(RfqMetadataFieldNames.volumeOfInstrumentWithEntityPastYear);

        assertEquals(1_350_000L, result);
    }

    @Test
    public void checkVolumeWhenNoTradesMatchYTD() {

        String filePath = getClass().getResource("volume-traded-1.json").getPath();
        Dataset<Row> trades = new TradeDataLoader().loadTrades(session, filePath);

        //all test trade data are for 2018 so this will cause no matches
        VolumeTradedByInstrumentOfEntityExtractor extractor = new VolumeTradedByInstrumentOfEntityExtractor();
        extractor.setSince("2020-01-01");

        Map<RfqMetadataFieldNames, Object> meta = extractor.extractMetaData(rfq, session, trades);

        Object result = meta.get(RfqMetadataFieldNames.volumeOfInstrumentWithEntityPastYear);

        assertEquals(0L, result);
    }

    @Test
    public void checkVolumeWhenAllTradesMatchMTD() {

        String filePath = getClass().getResource("volume-traded-1.json").getPath();
        Dataset<Row> trades = new TradeDataLoader().loadTrades(session, filePath);

        VolumeTradedByInstrumentOfEntityExtractor extractor = new VolumeTradedByInstrumentOfEntityExtractor();
        extractor.setSince("2018-06-10");

        Map<RfqMetadataFieldNames, Object> meta = extractor.extractMetaData(rfq, session, trades);

        Object result = meta.get(RfqMetadataFieldNames.volumeOfInstrumentWithEntityPastMonth);
        assertEquals(600_000L, result);
    }

    @Test
    public void checkVolumeWhenNoTradesMatchMTD() {

        String filePath = getClass().getResource("volume-traded-1.json").getPath();
        Dataset<Row> trades = new TradeDataLoader().loadTrades(session, filePath);

        //all test trade data are for 2018 so this will cause no matches
        VolumeTradedByInstrumentOfEntityExtractor extractor = new VolumeTradedByInstrumentOfEntityExtractor();
        extractor.setSince("2019-06-07");

        Map<RfqMetadataFieldNames, Object> meta = extractor.extractMetaData(rfq, session, trades);

        Object result = meta.get(RfqMetadataFieldNames.volumeOfInstrumentWithEntityPastMonth);

        assertEquals(0L, result);
    }

    @Test
    public void checkVolumeWhenAllTradesMatchWTD() {

        String filePath = getClass().getResource("volume-traded-1.json").getPath();
        Dataset<Row> trades = new TradeDataLoader().loadTrades(session, filePath);

        VolumeTradedByInstrumentOfEntityExtractor extractor = new VolumeTradedByInstrumentOfEntityExtractor();
        extractor.setSince("2018-06-10");

        Map<RfqMetadataFieldNames, Object> meta = extractor.extractMetaData(rfq, session, trades);

        Object result = meta.get(RfqMetadataFieldNames.volumeOfInstrumentWithEntityPastWeek);
        assertEquals(600_000L, result);
    }

    @Test
    public void checkVolumeWhenNoTradesMatchWTD() {

        String filePath = getClass().getResource("volume-traded-1.json").getPath();
        Dataset<Row> trades = new TradeDataLoader().loadTrades(session, filePath);

        //all test trade data are for 2018 so this will cause no matches
        VolumeTradedByInstrumentOfEntityExtractor extractor = new VolumeTradedByInstrumentOfEntityExtractor();
        extractor.setSince("2019-06-07");

        Map<RfqMetadataFieldNames, Object> meta = extractor.extractMetaData(rfq, session, trades);

        Object result = meta.get(RfqMetadataFieldNames.volumeOfInstrumentWithEntityPastWeek);

        assertEquals(0L, result);
    }
}
