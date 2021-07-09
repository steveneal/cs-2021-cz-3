package com.cs.rfq.decorator.extractors;

import com.cs.rfq.decorator.Rfq;
import com.cs.rfq.decorator.TradeDataLoader;
import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Row;
import org.junit.Before;
import org.junit.Test;

import java.util.Map;

import static org.junit.Assert.assertEquals;

public class InstrumentLiquidityExtractorTest extends AbstractSparkUnitTest {

    private Rfq rfq;

    @Before
    public void setup() {
        rfq = new Rfq();
        rfq.setEntityId(5561279226039690843L);
        rfq.setIsin("AT0000A0VRQ6");
    }

    @Test
    public void checkVolumeWhenAllTradesMatch() {
        String filePath = getClass().getResource("volume-traded-2.json").getPath();
        Dataset<Row> trades = new TradeDataLoader().loadTrades(session, filePath);

        InstrumentLiquidityExtractor extractor = new InstrumentLiquidityExtractor("2018-07-01");

        Map<RfqMetadataFieldNames, Object> meta = extractor.extractMetaData(rfq, session, trades);

        Object result = meta.get(RfqMetadataFieldNames.instrumentLiquidity);

        assertEquals(550_000L, result);
    }

    @Test
    public void checkVolumeWhenNoTradesMatch() {
        String filePath = getClass().getResource("volume-traded-2.json").getPath();
        Dataset<Row> trades = new TradeDataLoader().loadTrades(session, filePath);

        InstrumentLiquidityExtractor extractor = new InstrumentLiquidityExtractor();

        Map<RfqMetadataFieldNames, Object> meta = extractor.extractMetaData(rfq, session, trades);

        Object result = meta.get(RfqMetadataFieldNames.instrumentLiquidity);

        assertEquals(0L, result);
    }

}
