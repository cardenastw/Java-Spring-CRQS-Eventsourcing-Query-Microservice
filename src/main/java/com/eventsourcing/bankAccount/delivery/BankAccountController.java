package com.eventsourcing.bankAccount.delivery;


import com.eventsourcing.bankAccount.dto.BankAccountResponseDTO;
import com.eventsourcing.bankAccount.queries.BankAccountQueryService;
import com.eventsourcing.bankAccount.queries.FindAllOrderByBalance;
import com.eventsourcing.bankAccount.queries.GetBankAccountByIDQuery;
import lombok.RequiredArgsConstructor;
import lombok.extern.slf4j.Slf4j;
import org.springframework.data.domain.Page;
import org.springframework.http.ResponseEntity;
import org.springframework.web.bind.annotation.GetMapping;
import org.springframework.web.bind.annotation.PathVariable;
import org.springframework.web.bind.annotation.RequestMapping;
import org.springframework.web.bind.annotation.RequestParam;
import org.springframework.web.bind.annotation.RestController;

import java.time.Duration;
import java.time.Instant;
import java.time.format.DateTimeParseException;

@RestController
@RequestMapping(path = "/api/v1/bank")
@Slf4j
@RequiredArgsConstructor
public class BankAccountController {

    private final BankAccountQueryService queryService;

    @GetMapping("{aggregateId}")
    public ResponseEntity<BankAccountResponseDTO> getBankAccount(@PathVariable String aggregateId,
                                                                 @RequestParam(required = false) String asOf) {
        Instant timestamp = Instant.now(); // Default to current time

        if (asOf != null) {
            try {
                timestamp = Instant.parse(asOf); // Parse provided timestamp

                // Ensure asOf is not older than 30 days
                Instant oldestAllowed = Instant.now().minus(Duration.ofDays(30));
                if (timestamp.isBefore(oldestAllowed)) {
                    throw new IllegalArgumentException("The requested timestamp is older than 30 days and is not allowed.");
                }
            } catch (DateTimeParseException e) {
                throw new IllegalArgumentException("Invalid date format. Use ISO-8601 (e.g., 2024-01-31T12:34:56Z)");
            }
        }

        final var result = queryService.handle(new GetBankAccountByIDQuery(aggregateId, timestamp));
        log.info("Get bank account result: {}", result);
        return ResponseEntity.ok(result);
    }

    @GetMapping("/balance")
    public ResponseEntity<Page<BankAccountResponseDTO>> getAllOrderByBalance(@RequestParam(name = "page", defaultValue = "0") Integer page,
                                                                             @RequestParam(name = "size", defaultValue = "10") Integer size) {

        final var result = queryService.handle(new FindAllOrderByBalance(page, size));
        log.info("Get all by balance page: {}, size: {}, result: {}", page, size, result);
        return ResponseEntity.ok(result);
    }
}
