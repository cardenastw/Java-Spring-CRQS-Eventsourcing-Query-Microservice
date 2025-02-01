package com.eventsourcing.bankAccount.repository;

import com.eventsourcing.bankAccount.domain.BankAccountHistoryDocument;
import org.springframework.data.mongodb.repository.MongoRepository;

import java.util.Optional;

public interface BankAccountMongoRepository extends MongoRepository<BankAccountHistoryDocument, String> {

    Optional<BankAccountHistoryDocument> findByAggregateId(String aggregateId);

    void deleteByAggregateId(String aggregateId);
}
