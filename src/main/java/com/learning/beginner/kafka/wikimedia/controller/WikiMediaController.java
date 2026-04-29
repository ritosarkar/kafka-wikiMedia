package com.learning.beginner.kafka.wikimedia.controller;

import com.launchdarkly.eventsource.StreamException;
import com.learning.beginner.kafka.wikimedia.service.WikiMediaProducerService;
import lombok.RequiredArgsConstructor;
import org.springframework.web.bind.annotation.GetMapping;
import org.springframework.web.bind.annotation.PostMapping;
import org.springframework.web.bind.annotation.RestController;

@RestController
@RequiredArgsConstructor
public class WikiMediaController {
    public final WikiMediaProducerService wikiMediaProducerService;

   @GetMapping("/produceDataWikiMedia")
    public void produceWikiMedData() throws InterruptedException, StreamException {
     wikiMediaProducerService.startStreaming();
   }
}
