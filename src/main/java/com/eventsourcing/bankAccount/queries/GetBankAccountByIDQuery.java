package com.eventsourcing.bankAccount.queries;

import java.time.Instant;

public record GetBankAccountByIDQuery(String aggregateID, Instant asOf) {
}
