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
        properties.setProperty(ProducerConfig.KEY_SERIALIZER_CLASS_CONFIG, StringSerializer.class.getName());
        properties.setProperty(ProducerConfig.VALUE_SERIALIZER_CLASS_CONFIG, StringSerializer.class.getName());
        //Set high throughput producer configs
        properties.setProperty(ProducerConfig.LINGER_MS_CONFIG, appConfiguration.getLingerMs());
        properties.setProperty(ProducerConfig.BATCH_SIZE_CONFIG, appConfiguration.getBatchSize());
        properties.setProperty(ProducerConfig.COMPRESSION_TYPE_CONFIG, appConfiguration.getCompressionType());
        /*
        properties.setProperty("batch.size","400");
        properties.setProperty("partitioner.class",RoundRobinPartitioner.class.getName());

        If kafka version is <=2.8 then we have to configure below properties -
        properties.setProperty(ProducerConfig.ENABLE_IDEMPOTENCE_CONFIG, appConfiguration.getEnableIdempotence());
        properties.setProperty(ProducerConfig.RETRIES_CONFIG, Integer.toString(Integer.MAX_VALUE));
        properties.setProperty(ProducerConfig.ACKS_CONFIG, appConfiguration.getAcknowledgements());
        */
        return new KafkaProducer<>(properties);
    }
}
