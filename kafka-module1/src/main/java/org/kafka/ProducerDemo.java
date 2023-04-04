package org.kafka;

import org.apache.kafka.clients.producer.KafkaProducer;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.apache.kafka.common.serialization.StringSerializer;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.Properties;

public class ProducerDemo {

    private static final Logger logger = LoggerFactory.getLogger(ProducerDemo.class.getSimpleName());
    public static void main(String[] args) {
       logger.info("Hello World");
        Properties properties = new Properties();

        //set kafka server connect
        properties.setProperty("bootstrap.servers", "localhost:9092");

        //set producer properties
        properties.setProperty("key.serializer", StringSerializer.class.getName());
        properties.setProperty("value.serializer", StringSerializer.class.getName());

        KafkaProducer<String, String> producer = new KafkaProducer<String, String>(properties);


        //create message
        ProducerRecord<String, String> producerRecord =
                new ProducerRecord<String, String>("first_topic", "All is well");

        //send message
        producer.send(producerRecord);

        producer.flush();

        producer.close();

        logger.info("Already send message");
    }
}
