package de.malte.kafkacampfire.config;

import org.apache.kafka.clients.admin.NewTopic;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;

@Configuration
public class KafkaTopicConfig {

    @Value("${kafka.topic.streams.output}")
    private String kafkaTopicStreamsOutput;

    @Value("${kafka.topic.transaction.input}")
    private String kafkaTopicTransactionInput;

    @Bean
    public NewTopic streamsOutputTopic() {
        return new NewTopic(kafkaTopicStreamsOutput, 2, (short) 2);
    }

    @Bean
    public NewTopic transactionInputTopic() {
        return new NewTopic(kafkaTopicTransactionInput, 2, (short) 2);
    }
}
