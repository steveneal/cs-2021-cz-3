package dev.testdata;

public class Instrument {
    String isin;
    Double price;

    public Instrument(String isin, Double price) {
        this.isin = isin;
        this.price = price;
    }

    public static boolean validateCsv(String line) {
        String[] arr = line.split(",");
        return (arr[9].matches("[0-9].*"));
    }

    public static Instrument fromCsv(String line) {
        String[] arr = line.split(",");
        return new Instrument(arr[0], Double.parseDouble(arr[9]));
    }

    @Override
    public String toString() {
        return "Instrument{" +
                "isin='" + isin + '\'' +
                ", price=" + price +
                '}';
    }
}
