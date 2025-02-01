package com.eventsourcing.mappers;


import com.eventsourcing.bankAccount.domain.BankAccountPojo;
import com.eventsourcing.bankAccount.dto.BankAccountResponseDTO;

public final class BankAccountMapper {

    private BankAccountMapper() {
    }

    public static BankAccountResponseDTO bankAccountResponseDTOFromDocument(BankAccountPojo bankAccountDocument) {
        return new BankAccountResponseDTO(
                bankAccountDocument.getAggregateId(),
                bankAccountDocument.getEmail(),
                bankAccountDocument.getAddress(),
                bankAccountDocument.getUserName(),
                bankAccountDocument.getBalance()
        );
    }
}