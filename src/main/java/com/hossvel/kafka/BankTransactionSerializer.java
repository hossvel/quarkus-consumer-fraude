package com.hossvel.kafka;

import com.hossvel.model.BankTransaction;
import io.quarkus.kafka.client.serialization.ObjectMapperDeserializer;


public class BankTransactionSerializer extends ObjectMapperDeserializer<BankTransaction> {
    public BankTransactionSerializer() {
        super(BankTransaction.class);
    }
}