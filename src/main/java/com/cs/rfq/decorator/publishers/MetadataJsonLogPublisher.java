package com.cs.rfq.decorator.publishers;

import com.cs.rfq.decorator.extractors.RfqMetadataFieldNames;
import com.google.gson.Gson;
import com.google.gson.GsonBuilder;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.File;
import java.io.FileWriter;
import java.io.IOException;
import java.util.Map;

public class MetadataJsonLogPublisher implements MetadataPublisher {

    private static final Logger log = LoggerFactory.getLogger(MetadataJsonLogPublisher.class);

    @Override
    public void publishMetadata(Map<RfqMetadataFieldNames, Object> metadata) {
        Gson gson = new GsonBuilder().setPrettyPrinting().create();
        //String s = new GsonBuilder().setPrettyPrinting().create().toJson(metadata);
        //log.info(String.format("Publishing metadata:%n%s", s));
        FileWriter fw = null;
        try {
            fw = new FileWriter(new File("src/test/resources/trades/trades_output.json").getAbsolutePath() );
        } catch (IOException e) {
            e.printStackTrace();
        }
        gson.toJson(metadata, fw);
        try {
            fw.close();
        } catch (IOException e) {
            e.printStackTrace();
        }

    }
}
