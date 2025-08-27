package com.hossvel.model;

import java.time.Instant;

public class BankTransaction {

    private Long id;

    private String accountId;
    private double amount;
    private String type; // "DEPOSIT" or "WITHDRAWAL"
    private Instant timestamp;

    public BankTransaction() {
    }

    public BankTransaction(String accountId, double amount, String type) {
        this.accountId = accountId;
        this.amount = amount;
        this.type = type;
    }

    public Long getId() {
        return id;
    }

    public void setId(Long id) {
        this.id = id;
    }

    public String getAccountId() {
        return accountId;
    }

    public void setAccountId(String accountId) {
        this.accountId = accountId;
    }

    public double getAmount() {
        return amount;
    }

    public void setAmount(double amount) {
        this.amount = amount;
    }

    public String getType() {
        return type;
    }

    public void setType(String type) {
        this.type = type;
    }

    public Instant getTimestamp() {
        return timestamp;
    }

    public void setTimestamp(Instant timestamp) {
        this.timestamp = timestamp;
    }
}
