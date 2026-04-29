package com.learning.beginner.kafka.wikimedia.config;

import lombok.RequiredArgsConstructor;
import org.apache.kafka.clients.producer.KafkaProducer;
import org.apache.kafka.clients.producer.ProducerConfig;
import org.apache.kafka.common.serialization.StringSerializer;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;

import java.util.Properties;

@Configuration
@RequiredArgsConstructor
public class ProducerConfigWikiMedia {
    private final AppConfiguration appConfiguration;

    @Bean
    public KafkaProducer<String, String> getProducerProperties() {
        Properties properties = new Properties();
        //connect to localhost 172.18.0.3 or 127.0.0.1
        properties.setProperty(ProducerConfig.BOOTSTRAP_SERVERS_CONFIG, appConfiguration.getBootstrapServers());
        properties.setProperty(ProducerConfig.KEY_SERIALIZER_CLASS_CONFIG,StringSerializer.class.getName());
        properties.setProperty(ProducerConfig.VALUE_SERIALIZER_CLASS_CONFIG,StringSerializer.class.getName());
        /*
        properties.setProperty("batch.size","400");
        properties.setProperty("partitioner.class",RoundRobinPartitioner.class.getName());
        */
        return new KafkaProducer<>(properties);
    }
}
