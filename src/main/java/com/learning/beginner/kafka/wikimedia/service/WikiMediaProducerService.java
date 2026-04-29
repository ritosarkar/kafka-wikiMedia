package com.learning.beginner.kafka.wikimedia.service;


import com.launchdarkly.eventsource.EventSource;
import com.launchdarkly.eventsource.StreamException;
import com.launchdarkly.eventsource.background.BackgroundEventSource;
import com.learning.beginner.kafka.wikimedia.components.WikiMediaProducerHandler;
import com.learning.beginner.kafka.wikimedia.config.AppConfiguration;
import jakarta.annotation.PostConstruct;
import lombok.RequiredArgsConstructor;
import org.springframework.stereotype.Service;


import java.net.URI;
import java.util.concurrent.TimeUnit;


@Service
@RequiredArgsConstructor
public class WikiMediaProducerService {
    private final AppConfiguration appConfiguration;
    private final WikiMediaProducerHandler wikiMediaProducerHandler;

    public void startStreaming() throws InterruptedException{
        BackgroundEventSource.Builder backGroundEventSource=
                new BackgroundEventSource.Builder(wikiMediaProducerHandler,
                        new EventSource.Builder(URI.create(appConfiguration.getStreamUrl())));
        try (BackgroundEventSource eventSource = backGroundEventSource.build()) {
            eventSource.start();
        }
        TimeUnit.SECONDS.sleep(10);
    }
}
