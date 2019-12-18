package com.cwoongc.kafka.kafkacommittest.message;

import lombok.AllArgsConstructor;
import lombok.Builder;
import lombok.Data;
import lombok.NoArgsConstructor;

import java.math.BigDecimal;
import java.util.List;

@Data
@Builder
@AllArgsConstructor
@NoArgsConstructor
public class WithdrawalPendingApproval {

    private String statusCode;
    private String requestId;
    private String organizationId;
    private String walletId;

}
