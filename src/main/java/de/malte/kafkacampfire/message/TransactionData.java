package de.malte.kafkacampfire.message;

import lombok.*;

import java.util.Date;

@Getter
@Setter
@Builder
@AllArgsConstructor
@NoArgsConstructor
@ToString
public class TransactionData {

    private String transactionID;
    private String cardNumber;
    private Double amount;
    private Date timestamp;
    private String email;
    private String name;
    private String address;
    private String postalZip;
    private String country;
    private String latlng;
    private String phone;
    private String region;
    private String comment;
}