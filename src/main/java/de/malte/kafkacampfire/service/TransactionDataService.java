package de.malte.kafkacampfire.service;

import de.malte.kafkacampfire.message.TransactionData;
import com.fasterxml.jackson.core.type.TypeReference;
import com.fasterxml.jackson.databind.ObjectMapper;
import jakarta.annotation.PostConstruct;
import lombok.extern.slf4j.Slf4j;
import org.springframework.stereotype.Service;

import java.io.IOException;
import java.io.InputStream;
import java.util.List;
import java.util.Random;

@Service
@Slf4j
public class TransactionDataService {

    private List<TransactionData> transactionDataList;

    @PostConstruct
    private void init() {
        ObjectMapper mapper = new ObjectMapper();
        TypeReference<List<TransactionData>> typeReference = new TypeReference<>() {};
        InputStream inputStream = TypeReference.class.getResourceAsStream("/data/sample.json");
        try {
            transactionDataList = mapper.readValue(inputStream, typeReference);
            log.info("Successfully loaded TransactionData from classpath resource.");
        } catch (IOException e){
            log.info("Unable to load TransactionData from classpath resource: " + e.getMessage());
        }
    }

    public TransactionData getRandomTransactionData() {
        return transactionDataList.get(new Random().nextInt(6));
    }
}