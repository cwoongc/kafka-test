package com.cwoongc.kafka.kafkacommittest.message;

import lombok.AllArgsConstructor;
import lombok.Builder;
import lombok.Data;
import lombok.NoArgsConstructor;

@Data
@Builder
@AllArgsConstructor
@NoArgsConstructor
public class WithdrawalApproved {

    private String statusCode;
    private String requestId;
    private String organizationId;
    private String walletId;

    private String coinCode;

}
