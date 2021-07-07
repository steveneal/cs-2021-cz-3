package dev.testdata;

public class Counterparty {
    long traderId;
    long entityId;

    public Counterparty(long traderId, long entityId) {
        this.traderId = traderId;
        this.entityId = entityId;
    }

    public static Counterparty fromCsv(String line) {
        String[] arr = line.split(",");
        return new Counterparty(Long.parseLong(arr[0]),Long.parseLong(arr[2]));
    }

    @Override
    public String toString() {
        return "Counterparty{" +
                "traderId=" + traderId +
                ", entityId=" + entityId +
                '}';
    }
}
