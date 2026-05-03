package com.learning.beginner.kafka.wikimedia.config;

import lombok.Data;
import org.springframework.boot.context.properties.ConfigurationProperties;
import org.springframework.context.annotation.Configuration;
import org.springframework.stereotype.Component;

import java.util.List;

@Component
@Data
@Configuration
@ConfigurationProperties(prefix = "spring.kafka")
public class AppConfiguration {
    private String bootstrapServers;
    private List<String> topic;
    private String streamUrl;
    /*If kafka version is <=2.8 then we have to configure below properties -
    private String enableIdempotence;
    private String acknowledgements;
     */
    private String lingerMs;
    private String batchSize;
    private String compressionType;
}
