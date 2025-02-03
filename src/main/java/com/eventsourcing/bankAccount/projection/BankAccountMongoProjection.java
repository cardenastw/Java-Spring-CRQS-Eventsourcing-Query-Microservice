package com.eventsourcing.bankAccount.projection;


import com.eventsourcing.bankAccount.domain.BankAccountHistoryDocument;
import com.eventsourcing.bankAccount.domain.BankAccountPojo;
import com.eventsourcing.bankAccount.events.AddressUpdatedEvent;
import com.eventsourcing.bankAccount.events.BalanceDepositedEvent;
import com.eventsourcing.bankAccount.events.BankAccountCreatedEvent;
import com.eventsourcing.bankAccount.events.EmailChangedEvent;
import com.eventsourcing.bankAccount.exceptions.BankAccountDocumentNotFoundException;
import com.eventsourcing.bankAccount.repository.BankAccountMongoRepository;
import com.eventsourcing.es.Event;
import com.eventsourcing.es.Projection;
import com.eventsourcing.es.SerializerUtils;
import io.github.resilience4j.circuitbreaker.annotation.CircuitBreaker;
import io.github.resilience4j.retry.annotation.Retry;
import lombok.RequiredArgsConstructor;
import lombok.extern.slf4j.Slf4j;
import io.micrometer.tracing.annotation.NewSpan;
import io.micrometer.tracing.annotation.SpanTag;
import org.springframework.kafka.annotation.KafkaListener;
import org.springframework.kafka.listener.adapter.ConsumerRecordMetadata;
import org.springframework.kafka.support.Acknowledgment;
import org.springframework.messaging.handler.annotation.Payload;
import org.springframework.stereotype.Service;

import java.math.BigDecimal;
import java.time.Duration;
import java.time.Instant;
import java.util.Arrays;
import java.util.List;

@Service
@Slf4j
@RequiredArgsConstructor
public class BankAccountMongoProjection implements Projection {

    private final BankAccountMongoRepository mongoRepository;
    private static final String SERVICE_NAME = "microservice";


    @KafkaListener(topics = {"${microservice.kafka.topics.bank-account-event-store}"},
            groupId = "${microservice.kafka.groupId}-query",
            concurrency = "${microservice.kafka.default-concurrency}")
    public void bankAccountMongoProjectionListener(@Payload byte[] data, ConsumerRecordMetadata meta, Acknowledgment ack) {
        log.info("(BankAccountMongoProjection) topic: {}, offset: {}, partition: {}, timestamp: {}, data: {}", meta.topic(), meta.offset(), meta.partition(), meta.timestamp(), new String(data));

        try {
            final Event[] events = SerializerUtils.deserializeEventsFromJsonBytes(data);
            this.processEvents(Arrays.stream(events).toList());
            ack.acknowledge();
            log.info("ack events: {}", Arrays.toString(events));
        } catch (Exception ex) {
            ack.nack(Duration.ofMillis(100));
            log.error("(BankAccountMongoProjection) topic: {}, offset: {}, partition: {}, timestamp: {}", meta.topic(), meta.offset(), meta.partition(), meta.timestamp(), ex);
        }
    }

    @NewSpan
    private void processEvents(@SpanTag("events") List<Event> events) {
        if (events.isEmpty()) return;
		events.forEach(this::when);
    }

    @Override
    @NewSpan
    @Retry(name = SERVICE_NAME)
    @CircuitBreaker(name = SERVICE_NAME)
    public void when(@SpanTag("event") Event event) {
        final var aggregateId = event.getAggregateId();
        log.info("(when) >>>>> aggregateId: {}", aggregateId);

        switch (event.getEventType()) {
            case BankAccountCreatedEvent.BANK_ACCOUNT_CREATED_V1 ->
                    handle(SerializerUtils.deserializeFromJsonBytes(event.getData(), BankAccountCreatedEvent.class));
            case EmailChangedEvent.EMAIL_CHANGED_V1 ->
                    handle(SerializerUtils.deserializeFromJsonBytes(event.getData(), EmailChangedEvent.class));
            case AddressUpdatedEvent.ADDRESS_UPDATED_V1 ->
                    handle(SerializerUtils.deserializeFromJsonBytes(event.getData(), AddressUpdatedEvent.class));
            case BalanceDepositedEvent.BALANCE_DEPOSITED ->
                    handle(SerializerUtils.deserializeFromJsonBytes(event.getData(), BalanceDepositedEvent.class));
            default -> log.error("unknown event type: {}", event.getEventType());
        }
    }


    @NewSpan
    private void handle(@SpanTag("event") BankAccountCreatedEvent event) {
        log.info("(when) BankAccountCreatedEvent: {}, aggregateID: {}", event, event.getAggregateId());

        BankAccountHistoryDocument history = BankAccountHistoryDocument.builder()
                .aggregateId(event.getAggregateId())
                .build();
        history.addVersion(BankAccountPojo.builder()
                .aggregateId(event.getAggregateId())
                .email(event.getEmail())
                .address(event.getAddress())
                .userName(event.getUserName())
                .balance(BigDecimal.valueOf(0))
                .build(),
                Instant.now());

        final var insert = mongoRepository.insert(history);
        log.info("(BankAccountCreatedEvent) insert: {}", insert);
    }

    @NewSpan
    private void handle(@SpanTag("event") EmailChangedEvent event) {
        log.info("(when) EmailChangedEvent: {}, aggregateID: {}", event, event.getAggregateId());
        final var documentOptional = mongoRepository.findByAggregateId(event.getAggregateId());
        if (documentOptional.isEmpty())
            throw new BankAccountDocumentNotFoundException(event.getAggregateId());

        final var document = documentOptional.get();

        var nextVersion = document.getLatestVersion();
        nextVersion.setEmail(event.getNewEmail());

        document.addVersion(nextVersion, Instant.now());
        mongoRepository.save(document);
    }

    @NewSpan
    private void handle(@SpanTag("event") AddressUpdatedEvent event) {
        log.info("(when) AddressUpdatedEvent: {}, aggregateID: {}", event, event.getAggregateId());
        final var documentOptional = mongoRepository.findByAggregateId(event.getAggregateId());
        if (documentOptional.isEmpty())
            throw new BankAccountDocumentNotFoundException(event.getAggregateId());

        final var document = documentOptional.get();

        var nextVersion = document.getLatestVersion();
        nextVersion.setAddress(event.getNewAddress());

        document.addVersion(nextVersion, Instant.now());

        mongoRepository.save(document);
    }

    @NewSpan
    private void handle(@SpanTag("event") BalanceDepositedEvent event) {
        log.info("(when) BalanceDepositedEvent: {}, aggregateID: {}", event, event.getAggregateId());
        final var documentOptional = mongoRepository.findByAggregateId(event.getAggregateId());
        if (documentOptional.isEmpty())
            throw new BankAccountDocumentNotFoundException(event.getAggregateId());

        final var document = documentOptional.get();


        var nextVersion = document.getLatestVersion();
        final var newBalance = nextVersion.getBalance().add(event.getAmount());
        nextVersion.setBalance(newBalance);

        document.addVersion(nextVersion, Instant.now());

        mongoRepository.save(document);
    }
}
