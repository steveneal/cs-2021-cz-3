package com.cs.rfq.decorator.kafka;

import java.util.Properties;
import java.util.concurrent.ExecutionException;

import org.apache.kafka.clients.producer.*;
import org.apache.kafka.common.serialization.LongSerializer;
import org.apache.kafka.common.serialization.StringSerializer;

public class RunProducer {

    public static Producer<Long, String> createProducer() {
        Properties props = new Properties();
        props.put(ProducerConfig.BOOTSTRAP_SERVERS_CONFIG, IKafkaConstants.KAFKA_BROKERS);
        props.put(ProducerConfig.CLIENT_ID_CONFIG, IKafkaConstants.CLIENT_ID);
        props.put(ProducerConfig.KEY_SERIALIZER_CLASS_CONFIG, LongSerializer.class.getName());
        props.put(ProducerConfig.VALUE_SERIALIZER_CLASS_CONFIG, StringSerializer.class.getName());
        //props.put(ProducerConfig.PARTITIONER_CLASS_CONFIG, CustomPartitioner.class.getName());
        return new KafkaProducer<>(props);
    }

    public static void main(String[] args) {
        Producer<Long, String> producer = createProducer();

        ProducerRecord<Long, String> record1 = new ProducerRecord<Long, String>("bloomberg", "{'id':'BL001', 'isin':'AT0000383864', 'traderId':7514623710987345033, 'entityId':5561279226039690843, 'quantity':5000, 'price':10.6, 'side':'B'}");
        try {
            RecordMetadata metadata = producer.send(record1).get();
            System.out.println("Bloomberg record sent to partition " + metadata.partition() + " with offset " + metadata.offset());
        } catch (ExecutionException | InterruptedException e) {
            System.out.println("Error in sending record");
            System.out.println(e);
        }

        ProducerRecord<Long, String> record2 = new ProducerRecord<Long, String>("euronext", "{'id':'EN001', 'isin':'AT0000383864', 'traderId':7514623710987345033, 'entityId':5561279226039690843, 'quantity':6000, 'price':10.8, 'side':'S'}");
        try {
            RecordMetadata metadata = producer.send(record2).get();
            System.out.println("Euronext record sent to partition " + metadata.partition() + " with offset " + metadata.offset());
        } catch (ExecutionException | InterruptedException e) {
            System.out.println("Error in sending record");
            System.out.println(e);
        }
    }

}