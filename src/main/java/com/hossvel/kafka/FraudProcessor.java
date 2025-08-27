package com.hossvel.kafka;



import com.hossvel.model.BankTransaction;
import com.hossvel.model.HighRiskAccountWasDetected;
import com.hossvel.model.LowRiskAccountWasDetected;
import jakarta.enterprise.context.ApplicationScoped;
import org.eclipse.microprofile.reactive.messaging.Channel;
import org.eclipse.microprofile.reactive.messaging.Emitter;
import org.eclipse.microprofile.reactive.messaging.Incoming;
import org.eclipse.microprofile.reactive.messaging.Message;
import org.jboss.logging.Logger;

import java.util.concurrent.CompletionStage;

@ApplicationScoped
public class FraudProcessor {
    private static final Logger LOGGER = Logger.getLogger(FraudProcessor.class);

    @Channel("low-risk-alerts-out")
    Emitter<LowRiskAccountWasDetected> lowRiskEmitter;

    @Channel("high-risk-alerts-out")
    Emitter<HighRiskAccountWasDetected> highRiskEmitter;

    @Incoming("bank-transactions-in")
    public CompletionStage<Void> sendEventNotifications(Message<BankTransaction> message){
        BankTransaction event = message.getPayload();

        logBankAccountWasCreatedEvent(event);

        Integer fraudScore = calculateFraudScore(event.getAmount());

        logFraudScore(event.getAccountId(), fraudScore);

        if(fraudScore>50){
            logEmitEvent("HighRiskAccountWasDetected", event.getAccountId());
            highRiskEmitter.send(
                    new HighRiskAccountWasDetected(event.getAccountId())
            );
        }else if(fraudScore>20){
            logEmitEvent("LowRiskAccountWasDetected", event.getAccountId());
            lowRiskEmitter.send(
                    new LowRiskAccountWasDetected(event.getAccountId())
            );
        }
        return message.ack();
    }

    private Integer calculateFraudScore(double amount) {
        if (amount > 25000) {
            return 75;
        } else if (amount > 3000) {
            return 25;
        }

        return -1;
    }

    private void logBankAccountWasCreatedEvent(BankTransaction event) {
        LOGGER.infov(
                "Received BankAccountWasCreated - ID: {0} Balance: {1}",
                event.getAccountId(),
                event.getAmount()
        );
    }

    private void logFraudScore(String bankAccountId, Integer score) {
        LOGGER.infov(
                "Fraud score was calculated - ID: {0} Score: {1}",
                bankAccountId,
                score
        );
    }

    private void logEmitEvent(String eventName, String bankAccountId) {
        LOGGER.infov(
                "Sending a {0} event for bank account #{1}",
                eventName,
                bankAccountId
        );
    }
}