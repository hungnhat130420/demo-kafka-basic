package org.kafka;

import org.apache.kafka.clients.producer.*;
import org.apache.kafka.common.serialization.StringSerializer;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.Properties;

public class ProducerWithCallback {

    private static final Logger logger = LoggerFactory.getLogger(ProducerWithCallback.class.getSimpleName());

    public static void main(String[] args) {
        logger.info("Hello World");
        Properties properties = new Properties();

        //set kafka server connect
        properties.setProperty("bootstrap.servers", "localhost:9092");

        //set producer properties
        properties.setProperty("key.serializer", StringSerializer.class.getName());
        properties.setProperty("value.serializer", StringSerializer.class.getName());
        properties.setProperty("partitioner.class", RoundRobinPartitioner.class.getName());

        KafkaProducer<String, String> producer = new KafkaProducer<String, String>(properties);

        //create message
        ProducerRecord<String, String> producerRecord =
                new ProducerRecord<String, String>("first_topic", "Send message with callback");

        //send message
        producer.send(producerRecord, new Callback() {
            @Override
            public void onCompletion(RecordMetadata recordMetadata, Exception e) {
                if (e == null) {
                    logger.info("Received new metadata." +
                            "Topic: " + recordMetadata.topic() +
                            "Partition: " + recordMetadata.partition() +
                            "Offset: " + recordMetadata.offset() +
                            "Timestamp: " + recordMetadata.timestamp());

            }   else {
                    logger.error("Error while producing", e);
                }
            }
        });

        producer.flush();

        producer.close();

        logger.info("Already send message");
    }
}
