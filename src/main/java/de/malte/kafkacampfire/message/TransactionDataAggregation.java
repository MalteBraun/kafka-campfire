package de.malte.kafkacampfire.message;

import lombok.AllArgsConstructor;
import lombok.Data;
import lombok.NoArgsConstructor;
import lombok.ToString;

import java.time.LocalDateTime;

@Data
@NoArgsConstructor
@AllArgsConstructor
@ToString
public class TransactionDataAggregation {

    private String customerID;
    private Double amount;

    private LocalDateTime periodStart;
    private LocalDateTime periodEnd;

    public TransactionDataAggregation(final String customerID, final Double amount) {
        this(customerID, amount, null, null);
    }
}