package de.malte.kafkacampfire.config;

import com.fasterxml.jackson.databind.ObjectMapper;
import de.malte.kafkacampfire.message.TransactionData;
import de.malte.kafkacampfire.message.TransactionDataAggregation;
import lombok.extern.slf4j.Slf4j;
import org.apache.kafka.common.serialization.Serde;
import org.apache.kafka.common.serialization.Serdes;
import org.apache.kafka.common.utils.Bytes;
import org.apache.kafka.streams.KeyValue;
import org.apache.kafka.streams.StreamsBuilder;
import org.apache.kafka.streams.StreamsConfig;
import org.apache.kafka.streams.kstream.*;
import org.apache.kafka.streams.state.KeyValueStore;
import org.apache.kafka.streams.state.Stores;
import org.apache.kafka.streams.state.WindowStore;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;
import org.springframework.kafka.annotation.EnableKafkaStreams;
import org.springframework.kafka.annotation.KafkaStreamsDefaultConfiguration;
import org.springframework.kafka.config.KafkaStreamsConfiguration;
import org.springframework.kafka.support.serializer.JsonDeserializer;
import org.springframework.kafka.support.serializer.JsonSerializer;

import java.math.BigDecimal;
import java.math.RoundingMode;
import java.time.Duration;
import java.time.LocalDateTime;
import java.time.ZoneId;
import java.util.HashMap;
import java.util.Map;

@Configuration
@Slf4j
@EnableKafkaStreams
public class KafkaStreamsConfig {
    @Value("${spring.kafka.bootstrap-servers}")
    private String bootstrapServers;
    @Value("${kafka.topic.transaction.input}")
    private String kafkaTopicTransactionInput;
    @Value("${kafka.topic.streams.output}")
    private String kafkaTopicStreamsOutput;
    private static final String AMOUNT_STORE_NAME = "amount_store";
    private static final String WINDOWED_AMOUNT_STORE_NAME = "windowed_amount_store";
    private static final String WINDOWED_AMOUNT_SUPPRESS_NODE_NAME = "windowed_amount_suppress";
    private final Duration windowDuration;
    private final ObjectMapper mapper;
    private final Serde<String> stringSerde;
    private final Serde<Double> doubleSerde;
    private final Serde<TransactionData> transactionDataSerde;
    private final Consumed<String, TransactionData> transactionDataConsumed;
    private final Serde<TransactionDataAggregation> transactionDataAggregationSerde;
    private final Produced<String, TransactionDataAggregation> transactionDataAggregationProduced;

    public KafkaStreamsConfig(@Value("${stream.window.duration}") Duration windowDuration, ObjectMapper mapper) {
        this.windowDuration = windowDuration;
        this.mapper = mapper;
        this.stringSerde = Serdes.String();
        this.doubleSerde = Serdes.Double();
        this.transactionDataSerde = jsonSerde(TransactionData.class);
        this.transactionDataConsumed = Consumed.with(stringSerde, transactionDataSerde);
        this.transactionDataAggregationSerde = jsonSerde(TransactionDataAggregation.class);
        this.transactionDataAggregationProduced = Produced.with(stringSerde, transactionDataAggregationSerde);
    }

    @Bean(name = KafkaStreamsDefaultConfiguration.DEFAULT_STREAMS_CONFIG_BEAN_NAME)
    KafkaStreamsConfiguration kStreamsConfig() {
        Map<String, Object> props = new HashMap<>();
        props.put(StreamsConfig.APPLICATION_ID_CONFIG, "kafka-streams-app");
        props.put(StreamsConfig.BOOTSTRAP_SERVERS_CONFIG, bootstrapServers);
        props.put(StreamsConfig.DEFAULT_KEY_SERDE_CLASS_CONFIG, Serdes.String().getClass().getName());
        props.put(StreamsConfig.DEFAULT_VALUE_SERDE_CLASS_CONFIG, Serdes.String().getClass().getName());

        return new KafkaStreamsConfiguration(props);
    }

    @Bean
    public KStream<String, TransactionDataAggregation> kStream(StreamsBuilder streamsBuilder) {
        var transactionDataByUser = streamsBuilder.stream(kafkaTopicTransactionInput, transactionDataConsumed).groupByKey();
        var aggregatedTransactionDataByUser = aggregate(transactionDataByUser);
        aggregatedTransactionDataByUser.to(kafkaTopicStreamsOutput, transactionDataAggregationProduced);
        return aggregatedTransactionDataByUser;
    }

    private KStream<String, TransactionDataAggregation> aggregate(KGroupedStream<String, TransactionData> transactionDataByUser) {
        if (windowDuration.isZero()) {
            return transactionDataByUser
                    .aggregate(this::initialize, this::aggregateAmount, materializedAsPersistentStore(AMOUNT_STORE_NAME, stringSerde, doubleSerde))
                    .toStream()
                    .mapValues(TransactionDataAggregation::new);
        }

        return transactionDataByUser.windowedBy(TimeWindows.of(windowDuration).grace(Duration.ZERO))
                .aggregate(this::initialize, this::aggregateAmount, materializedAsWindowStore(WINDOWED_AMOUNT_STORE_NAME, stringSerde, doubleSerde))
                .suppress(Suppressed.untilWindowCloses(Suppressed.BufferConfig.unbounded()).withName(WINDOWED_AMOUNT_SUPPRESS_NODE_NAME))
                .toStream()
                .map((key, aggregatedAmount) -> {
                    var start = LocalDateTime.ofInstant(key.window().startTime(), ZoneId.systemDefault());
                    var end = LocalDateTime.ofInstant(key.window().endTime(), ZoneId.systemDefault());
                    return KeyValue.pair(key.key(), new TransactionDataAggregation(key.key(), aggregatedAmount, start, end));
                });
    }



    private Double initialize() {
        return 0d;
    }

    private Double aggregateAmount(String key, TransactionData transactionData, Double aggregatedAmount) {
        return new BigDecimal(aggregatedAmount + transactionData.getAmount()).setScale(2, RoundingMode.HALF_UP).doubleValue();
    }

    private <T> Serde<T> jsonSerde(Class<T> targetClass) {
        return Serdes.serdeFrom(new JsonSerializer<>(mapper),
                new JsonDeserializer<>(targetClass, mapper, false));
    }

    private <K, V>Materialized<K, V, KeyValueStore<Bytes, byte[]>> materializedAsPersistentStore(
            String storeName,
            Serde<K> keySerde,
            Serde<V> valueSerde) {
        return Materialized.<K, V>as(Stores.persistentKeyValueStore(storeName))
                .withKeySerde(keySerde)
                .withValueSerde(valueSerde);
    }

    private <K, V> Materialized<K, V, WindowStore<Bytes, byte[]>> materializedAsWindowStore(
            String storeName,
            Serde<K> keySerde,
            Serde<V> valueSerde) {
        return Materialized.<K, V>as(Stores.persistentWindowStore(storeName, windowDuration, windowDuration, false))
                .withKeySerde(keySerde)
                .withValueSerde(valueSerde);
    }
}