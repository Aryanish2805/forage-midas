package com.jpmc.midascore;

import com.fasterxml.jackson.databind.ObjectMapper;
import com.jpmc.midascore.entity.TransactionRecord;
import com.jpmc.midascore.entity.UserRecord;
import com.jpmc.midascore.foundation.Transaction;
import com.jpmc.midascore.repository.TransactionRepository;
import com.jpmc.midascore.repository.UserRepository;
import org.springframework.kafka.annotation.KafkaListener;
import org.springframework.stereotype.Component;
import org.springframework.transaction.annotation.Transactional;

@Component
public class KafkaConsumer {

    private final ObjectMapper objectMapper = new ObjectMapper();
    private final UserRepository userRepository;
    private final TransactionRepository transactionRepository;

    public KafkaConsumer(
            UserRepository userRepository,
            TransactionRepository transactionRepository
    ) {
        this.userRepository = userRepository;
        this.transactionRepository = transactionRepository;
    }

    @KafkaListener(
            topics = "${general.kafka-topic}",
            groupId = "midas-group"
    )
    @Transactional
    public void listen(String message) throws Exception {

        // Deserialize JSON → Transaction
        Transaction transaction =
                objectMapper.readValue(message, Transaction.class);

        // Load users (repo returns entity or null)
        UserRecord sender =
                userRepository.findById(transaction.getSenderId());

        UserRecord recipient =
                userRepository.findById(transaction.getRecipientId());

        // Invalid users
        if (sender == null || recipient == null) {
            return;
        }

        // Insufficient balance
        if (sender.getBalance() < transaction.getAmount()) {
            return;
        }

        // Update balances
        sender.setBalance(sender.getBalance() - transaction.getAmount());
        recipient.setBalance(recipient.getBalance() + transaction.getAmount());

        // Save transaction
        TransactionRecord record =
                new TransactionRecord(sender, recipient, transaction.getAmount());

        transactionRepository.save(record);
        userRepository.save(sender);
        userRepository.save(recipient);
    }
}