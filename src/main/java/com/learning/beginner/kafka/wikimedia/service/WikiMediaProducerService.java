package com.learning.beginner.kafka.wikimedia.service;


import com.launchdarkly.eventsource.ConnectStrategy;
import com.launchdarkly.eventsource.EventSource;
import com.launchdarkly.eventsource.background.BackgroundEventSource;
import com.learning.beginner.kafka.wikimedia.components.WikiMediaProducerHandler;
import com.learning.beginner.kafka.wikimedia.config.AppConfiguration;
import lombok.RequiredArgsConstructor;
import lombok.extern.slf4j.Slf4j;
import org.springframework.stereotype.Service;


import java.net.URI;
import java.util.concurrent.TimeUnit;


@Slf4j
@Service
@RequiredArgsConstructor
public class WikiMediaProducerService {
    private final AppConfiguration appConfiguration;
    private final WikiMediaProducerHandler wikiMediaProducerHandler;

    public void startStreaming() throws InterruptedException {
        /*BackgroundEventSource.Builder backGroundEventSource=
                new BackgroundEventSource.Builder(wikiMediaProducerHandler,
                        new EventSource.Builder( ConnectStrategy.http(URI.create(appConfiguration.getStreamUrl()))
                                .header("user-agent","kafka-stream")
                                .connectTimeout(30, TimeUnit.SECONDS)
                        )
                );*/
        BackgroundEventSource eventSource = new BackgroundEventSource.Builder(wikiMediaProducerHandler,
                new EventSource.Builder(ConnectStrategy.http(URI.create(appConfiguration.getStreamUrl()))
                        .header("user-agent", "kafka-stream")
                        .connectTimeout(30, TimeUnit.SECONDS)
                )
        ).build();
        eventSource.start();
    }
}
