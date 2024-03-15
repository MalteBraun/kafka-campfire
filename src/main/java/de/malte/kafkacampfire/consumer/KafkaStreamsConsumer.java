package de.malte.kafkacampfire.consumer;

import de.malte.kafkacampfire.message.TransactionDataAggregation;
import lombok.extern.slf4j.Slf4j;
import org.springframework.kafka.annotation.KafkaListener;
import org.springframework.kafka.support.Acknowledgment;
import org.springframework.messaging.handler.annotation.Payload;
import org.springframework.stereotype.Component;

@Component
@Slf4j
public class KafkaStreamsConsumer {

    @KafkaListener(topics = "${kafka.topic.streams.output}",
            groupId = "${kafka.stream.transaction.group.id}",
            concurrency = "2",
            containerFactory = "transactionDataConcurrentKafkaListenerContainerFactory")
    public void consumeAggregation(@Payload TransactionDataAggregation transactionDataAggregation, Acknowledgment acknowledgment) {
        log.info("Aggregation receiver: " + transactionDataAggregation.toString());
        acknowledgment.acknowledge();
    }
}