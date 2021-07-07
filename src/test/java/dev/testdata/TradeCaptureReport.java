package dev.testdata;

import org.joda.time.DateTime;
import org.joda.time.format.DateTimeFormat;
import org.joda.time.format.DateTimeFormatter;

import java.util.Random;

public class TradeCaptureReport {

        /*
        FIELDS:
        MsgType = 35 AE = trade capture
        TradeReportID : Unique identifier for the TradeCaptureReport Capture Report <AE>
        PreviouslyReported : Y/N
        SecurityID = isin value here
        SecurityIdSource = 4 - isin
        LastQty : 1000000 = 1m
        LastPx : float value - may be negative
        TradeDate : YYYYMMDD
        TransactTime : UTCTimestamp eg: YYYYMMDD-HH:MM:SS
        NoSides:  number of sides
        Side: 1 = buy, 2 = sell
        OrderID = must be unique
        Currency: 3 letter code: USD, EUR, GBP etc
         */

    private static Random r = new Random();

    //counterparty IDs
    long TraderId;
    long EntityId;

    final int MsgType = 35;
    long TradeReportId = Math.abs(r.nextLong());
    final String PreviouslyReported = "N";
    String SecurityID = "";// isin value here
    final int SecurityIdSource = 4;//4 = isin
    long LastQty = 0;//1000000 = 1m
    double LastPx = 0;//float value - may be negative
    String TradeDate = "";//YYYY-MM-DD
    String TransactTime = "";//UTCTimestamp eg: YYYYMMDD-HH:MM:SS
    final int NoSides = 1;//number of sides
    int Side = 1; //: 1 = buy, 2 = sell
    long OrderID = Math.abs(r.nextLong());//must be unique
    String Currency = "EUR";// 3 letter code: USD, EUR, GBP etc

    DateTimeFormatter tradeDateFormatter = DateTimeFormat.forPattern("yyyy-MM-dd");
    DateTimeFormatter transactTimeFormatter = DateTimeFormat.forPattern("YYYYMMdd-HH:mm:ss");

    public TradeCaptureReport(Counterparty c, Instrument i, DateTime d) {
        TraderId = c.traderId;
        EntityId = c.entityId;
        SecurityID = i.isin;
        LastQty = 50_000 * (int) ((Math.random() * 10) + 1); // 1-10 * 50_000
        LastPx = randomizePrice(i.price);
        TradeDate = tradeDateFormatter.print(d);
        TransactTime = transactTimeFormatter.print(d);
        Side = (Math.random() > 0.5) ? 1 : 2;
    }

    private static double randomizePrice(double start) {
        // will be +/- 0.5 rounded to 3dp
        start += (0.5 * Math.random()) * (Math.random() > 0.5 ? 1 : -1);
        start *= 1000;
        int result = (int) start;
        return result / 1000.0;
    }

    @Override
    public String toString() {
        return "{" +
                "'TraderId':" + TraderId +
                ", 'EntityId':" + EntityId +
                ", 'MsgType':" + MsgType +
                ", 'TradeReportId':" + TradeReportId +
                ", 'PreviouslyReported':'" + PreviouslyReported + '\'' +
                ", 'SecurityID':'" + SecurityID + '\'' +
                ", 'SecurityIdSource':" + SecurityIdSource +
                ", 'LastQty':" + LastQty +
                ", 'LastPx':" + LastPx +
                ", 'TradeDate':'" + TradeDate + '\'' +
                ", 'TransactTime':'" + TransactTime + '\'' +
                ", 'NoSides':" + NoSides +
                ", 'Side':" + Side +
                ", 'OrderID':" + OrderID +
                ", 'Currency':'" + Currency + '\'' +
                '}';
    }
}
