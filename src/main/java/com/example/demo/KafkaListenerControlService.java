package com.example.demo;


import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.kafka.config.KafkaListenerEndpointRegistry;
import org.springframework.kafka.listener.MessageListenerContainer;
import org.springframework.stereotype.Service;

@Service
public class KafkaListenerControlService {

    @Autowired
    private KafkaListenerEndpointRegistry registry;

    public void startListener(String listenerId) {
        MessageListenerContainer listenerContainer = registry.getListenerContainer(listenerId);


        if (listenerContainer != null && !listenerContainer.isRunning()) {
            listenerContainer.start();
        }
    }

    public MessageListenerContainer getListenerContainer(String listenerId) {
        return registry.getListenerContainer(listenerId);
    }

    public void stopListener(String listenerId) {
        MessageListenerContainer listenerContainer = registry.getListenerContainer(listenerId);
        if (listenerContainer != null && listenerContainer.isRunning()) {
            listenerContainer.stop();
        }
    }

    public void pauseListener(String listenerId) {
        MessageListenerContainer listenerContainer = registry.getListenerContainer(listenerId);
        if (listenerContainer != null && listenerContainer.isRunning()) {
            if (listenerContainer.isContainerPaused() ) {
                System.out.println("The container: " + listenerId + " is paused.");
            }
            else {
                listenerContainer.pause();
                System.out.println("Sent pause request to container:" + listenerId);
            }
        }
    }

    public void resumeListener(String listenerId) {
        MessageListenerContainer listenerContainer = registry.getListenerContainer(listenerId);

        if (listenerContainer != null && listenerContainer.isContainerPaused() ) {
                System.out.println("Resume container:  " + listenerId);
                listenerContainer.resume();
            }
            else {
                System.out.println("Skip container resume for container " + listenerId);
            }
        }
    }
