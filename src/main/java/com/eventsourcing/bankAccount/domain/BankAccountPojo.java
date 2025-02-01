package com.eventsourcing.bankAccount.domain;

import lombok.AllArgsConstructor;
import lombok.Builder;
import lombok.Data;
import lombok.NoArgsConstructor;

import java.math.BigDecimal;

@Data
@AllArgsConstructor
@NoArgsConstructor
@Builder
public class BankAccountPojo  implements Cloneable
{
    private String aggregateId;
    private String email;
    private String userName;
    private String address;
    private BigDecimal balance;

    @Override
    public BankAccountPojo clone() {
        try {
            return (BankAccountPojo) super.clone();
        } catch (CloneNotSupportedException e) {
            throw new RuntimeException("Clone not supported", e);
        }
    }
}
