package de.malte.kafkacampfire.producer;

import de.malte.kafkacampfire.config.KafkaProducerConfig;
import de.malte.kafkacampfire.message.TransactionData;
import de.malte.kafkacampfire.service.TransactionDataService;
import lombok.extern.slf4j.Slf4j;
import org.apache.kafka.common.protocol.types.Field;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.kafka.core.KafkaTemplate;
import org.springframework.scheduling.annotation.EnableScheduling;
import org.springframework.scheduling.annotation.Scheduled;
import org.springframework.stereotype.Component;

@Component
@EnableScheduling
@Slf4j
public class KafkaProducer {

    @Value("${kafka.topic.transaction.input}")
    private String kafkaTopicTransactionInput;

    private final KafkaTemplate<String, TransactionData> transactionDataKafkaTemplate;

    private final TransactionDataService transactionDataService;

    public KafkaProducer(KafkaTemplate<String, TransactionData> transactionDataKafkaTemplate, TransactionDataService transactionDataService) {
        this.transactionDataKafkaTemplate = transactionDataKafkaTemplate;
        this.transactionDataService = transactionDataService;
    }

    @Scheduled(fixedRate = 5000)
    public void producerTransactionData(){
        TransactionData transactionData = transactionDataService.getRandomTransactionData();
        String key = transactionData.getEmail();
        log.info("Sending message with key '{}' and payload '{}'", key, transactionData);
        transactionDataKafkaTemplate.send(kafkaTopicTransactionInput, key, transactionData);
    }

}
