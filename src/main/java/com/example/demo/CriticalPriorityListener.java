package com.example.demo;

import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.context.event.EventListener;
import org.springframework.kafka.annotation.KafkaListener;
import org.springframework.kafka.config.ConcurrentKafkaListenerContainerFactory;
import org.springframework.kafka.config.KafkaListenerContainerFactory;
import org.springframework.kafka.event.ListenerContainerIdleEvent;
import org.springframework.stereotype.Component;

import java.util.List;

@Component
public class CriticalPriorityListener {

    @Autowired
    KafkaListenerControlService kafkaListenerControlService;

    private static long lastPollTimeMs = 0;

    private static final Logger logger = LoggerFactory.getLogger(CriticalPriorityListener.class);


    @KafkaListener(id = Constants.CRITICAL_LISTENER_ID, topics = Constants.CRITICAL_TOPIC, groupId = "spring-kafka-demo-group-v1",
            containerFactory = "kafkaListenerContainerFactory", autoStartup = "false", batch = "true")
    public void consume(List<ConsumerRecord<String, String>> records) {
        lastPollTimeMs = System.currentTimeMillis();
        records.forEach(r-> System.out.println("Receive event:" + r.value()));
    }

    @EventListener
    public void eventHandler(ListenerContainerIdleEvent event) {
        System.out.println(this.getClass().getName() + ": No messages received for " + event.getIdleTime() + " milliseconds");
        // Pause current container
//        kafkaListenerControlService.pauseListener(Constants.CRITICAL_LISTENER_ID);
//        // Resume high container
//        kafkaListenerControlService.resumeListener(Constants.HIGH_LISTENER_ID);
    }
    public static long getLastPollTimeMs() {
        return lastPollTimeMs;
    }
}