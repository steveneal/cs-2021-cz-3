package com.cs.rfq.decorator.publishers;

import com.cs.rfq.decorator.Rfq;
import com.cs.rfq.decorator.extractors.RfqMetadataFieldNames;
import com.google.gson.Gson;
import com.google.gson.GsonBuilder;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.File;
import java.io.FileWriter;
import java.io.IOException;
import java.util.HashMap;
import java.util.Map;

public class MetadataJsonLogPublisher implements MetadataPublisher {

    private static final Logger log = LoggerFactory.getLogger(MetadataJsonLogPublisher.class);

    @Override
    public void publishMetadata(Rfq rfq, Map<RfqMetadataFieldNames, Object> metadata) throws IOException {
        Gson gson = new GsonBuilder().create();
        FileWriter fw = new FileWriter(new File("src/test/resources/output/" + rfq.getId() + ".json").getAbsolutePath());

        gson.toJson(rfq, fw);
        fw.write("\n");
        gson.toJson(metadata, fw);

        fw.close();

    }
}
