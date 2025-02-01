package com.eventsourcing.bankAccount.domain;

import lombok.AllArgsConstructor;
import lombok.Builder;
import lombok.Data;
import lombok.NoArgsConstructor;
import org.bson.codecs.pojo.annotations.BsonProperty;
import org.springframework.data.mongodb.core.mapping.Document;

import java.time.Instant;
import java.util.LinkedList;
import java.util.Optional;

@Data
@AllArgsConstructor
@NoArgsConstructor
@Builder
@Document(collection = "bankAccountHistory")
public class BankAccountHistoryDocument
{

    @BsonProperty(value = "_id")
    private String id;

    @BsonProperty(value = "aggregateId")
    private String aggregateId;

    @BsonProperty(value = "history")
    private LinkedList<HistoricalRecord> history = new LinkedList<>();

    public void addVersion(BankAccountPojo bankAccount, Instant timestamp) {
        // Remove old versions beyond 30 days
        Instant cutoff = Instant.now().minusSeconds(30L * 24 * 60 * 60); // 30 days
        history.removeIf(record -> record.timestamp.isBefore(cutoff));

        // Add new version
        history.add(new HistoricalRecord(timestamp, bankAccount.clone())); // Ensure deep copy
    }

    public Optional<BankAccountPojo> getVersionAt(Instant time) {
        // Get the most recent version before or at the requested timestamp
        return history.stream()
                .filter(record -> !record.timestamp.isAfter(time))
                .map(record -> record.bankAccount)
                .reduce((first, second) -> second); // Get the last (most recent);
    }

    public BankAccountPojo getLatestVersion() {
        if (history.isEmpty()) {
            return null;
        }
        return history.getLast().bankAccount;
    }


    private static class HistoricalRecord {
        private final Instant timestamp;
        private final BankAccountPojo bankAccount;

        public HistoricalRecord(Instant timestamp, BankAccountPojo bankAccount) {
            this.timestamp = timestamp;
            this.bankAccount = bankAccount;
        }
    }
}
