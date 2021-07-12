package com.cs.rfq.decorator.publishers;

import com.cs.rfq.decorator.Rfq;
import com.cs.rfq.decorator.extractors.RfqMetadataFieldNames;
import com.cs.rfq.decorator.kafka.IKafkaConstants;
import com.google.gson.Gson;
import com.google.gson.GsonBuilder;
import org.apache.kafka.clients.producer.*;
import org.apache.kafka.common.serialization.LongSerializer;
import org.apache.kafka.common.serialization.StringSerializer;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import scala.util.parsing.json.JSONObject;

import java.io.File;
import java.io.FileWriter;
import java.io.IOException;
import java.util.HashMap;
import java.util.Map;
import java.util.Properties;
import java.util.Set;
import java.util.concurrent.ExecutionException;

public class MetadataJsonLogPublisher implements MetadataPublisher {

    private static final Logger log = LoggerFactory.getLogger(MetadataJsonLogPublisher.class);

    @Override
    public void publishMetadata(Rfq rfq, Map<RfqMetadataFieldNames, Object> metadata) throws IOException {
        Gson gson = new GsonBuilder().create();
        FileWriter fw = new FileWriter(new File("src/test/resources/output/" + rfq.getId() + ".json").getAbsolutePath());

        String rfqJson = gson.toJson(rfq);
        String metadataJson = gson.toJson(metadata);

        String concat = rfqJson.substring(0, rfqJson.length()-1) + "," + metadataJson.substring(1);
        fw.write(concat);
        fw.close();

        Producer<Long, String> producer = createProducer();
        ProducerRecord<Long, String> record2 = new ProducerRecord<Long, String>("output", concat);
        try {
            RecordMetadata md = producer.send(record2).get();
            System.out.println("Record with metadata sent to partition " + md.partition() + " with offset " + md.offset());
        } catch (ExecutionException | InterruptedException e) {
            System.out.println("Error in sending record");
            System.out.println(e);
        }
    }

    public static Producer<Long, String> createProducer() {
        Properties props = new Properties();
        props.put(ProducerConfig.BOOTSTRAP_SERVERS_CONFIG, IKafkaConstants.KAFKA_BROKERS);
        props.put(ProducerConfig.CLIENT_ID_CONFIG, IKafkaConstants.CLIENT_ID);
        props.put(ProducerConfig.KEY_SERIALIZER_CLASS_CONFIG, LongSerializer.class.getName());
        props.put(ProducerConfig.VALUE_SERIALIZER_CLASS_CONFIG, StringSerializer.class.getName());
        //props.put(ProducerConfig.PARTITIONER_CLASS_CONFIG, CustomPartitioner.class.getName());
        return new KafkaProducer<>(props);
    }

}
