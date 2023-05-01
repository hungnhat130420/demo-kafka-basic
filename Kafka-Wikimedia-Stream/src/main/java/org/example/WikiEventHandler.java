package org.example;

import com.launchdarkly.eventsource.EventHandler;
import com.launchdarkly.eventsource.MessageEvent;
import org.apache.kafka.clients.producer.KafkaProducer;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.slf4j.Logger;

public class WikiEventHandler implements EventHandler {

    KafkaProducer<String, String> kafkaProducer;
    String topic;

    private static final Logger logger = org.slf4j.LoggerFactory
            .getLogger(WikiEventHandler.class.getSimpleName());

    public WikiEventHandler(KafkaProducer<String, String> kafkaProducer, String topic) {
        this.kafkaProducer = kafkaProducer;
        this.topic = topic;
    }

    @Override
    public void onOpen() throws Exception {

    }

    @Override
    public void onClosed() throws Exception {
        kafkaProducer.close();
    }



    @Override
    public void onMessage(String s, MessageEvent messageEvent) throws Exception {
        logger.info("Received message: " + s);
        logger.info("Received message: " + messageEvent.getData());
        kafkaProducer.send(new ProducerRecord<>(topic, messageEvent.getData()));
    }

    @Override
    public void onComment(String s) throws Exception {

    }

    @Override
    public void onError(Throwable throwable) {
        logger.error("Error while receiving message", throwable);
    }
}
