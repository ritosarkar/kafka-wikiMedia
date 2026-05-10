package com.learning.beginner.kafka.wikimedia.components;


import com.launchdarkly.eventsource.MessageEvent;
import com.launchdarkly.eventsource.background.BackgroundEventHandler;
import com.learning.beginner.kafka.wikimedia.config.Topics;
import lombok.Getter;
import org.apache.kafka.clients.producer.KafkaProducer;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.stereotype.Component;

import java.util.concurrent.TimeUnit;


@Component
@Getter
public class WikiMediaProducerHandler implements BackgroundEventHandler {
    private static final Logger log = LoggerFactory.getLogger(WikiMediaProducerHandler.class.getSimpleName());
    private final String wikimediaTopic;
    private final KafkaProducer<String, String> kafkaProducer;
    private final Topics topics;

    @Autowired
    public WikiMediaProducerHandler(KafkaProducer<String, String> kafkaProducer,
                                    Topics topics) {
        this.kafkaProducer = kafkaProducer;
        this.topics = topics;
        this.wikimediaTopic = this.topics.getTopic();
    }

    @Override
    public void onOpen() {
        //no operation needed.
        log.info("On open invoked!!!");
    }

    @Override
    public void onClosed() {
        kafkaProducer.close();
    }

    @Override
    public void onMessage(String s, MessageEvent messageEvent) throws InterruptedException {
        log.info("Handling event===> \n{} \n<-----content------> \n{} \nwill be processed.", s, messageEvent.getData());
        kafkaProducer.send(new ProducerRecord<>(wikimediaTopic, messageEvent.getData()));
        TimeUnit.SECONDS.sleep(3);
    }

    @Override
    public void onComment(String s) {
        log.info("Comment invoked {}", s);
    }

    @Override
    public void onError(Throwable throwable) {
       log.error("Error in stream reading {}", throwable.getMessage());
    }
}
