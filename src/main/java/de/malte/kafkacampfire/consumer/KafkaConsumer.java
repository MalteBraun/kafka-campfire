package de.malte.kafkacampfire.consumer;


import de.malte.kafkacampfire.message.TransactionData;
import lombok.extern.slf4j.Slf4j;
import org.apache.kafka.common.protocol.types.Field;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.kafka.annotation.KafkaListener;
import org.springframework.kafka.support.Acknowledgment;
import org.springframework.messaging.handler.annotation.Payload;
import org.springframework.messaging.simp.SimpMessagingTemplate;
import org.springframework.stereotype.Component;

import java.util.Date;

@Component
@Slf4j
public class KafkaConsumer {

    @Value("${stomp.topic}")
    private String stompTopic;

    private final SimpMessagingTemplate messagingTemplate;

    public KafkaConsumer(SimpMessagingTemplate messagingTemplate) {
        this.messagingTemplate = messagingTemplate;
    }

    @KafkaListener(
     topics = "${kafka.topic.transaction.input}",
     groupId = "${kafka.topic.transaction.group.id}",
     clientIdPrefix = "consumer-1",
     concurrency = "2",
     containerFactory = "transactionDataConcurrentKafkaListenerContainerFactory"
     )
    public void consumer (@Payload TransactionData transactionData, Acknowledgment acknowledgment) {
         log.info(" Consumer: recieved key " +transactionData.getEmail() + " with payload " + transactionData);
         messagingTemplate.convertAndSend(stompTopic, transactionData);
         acknowledgment.acknowledge();
     }
}
