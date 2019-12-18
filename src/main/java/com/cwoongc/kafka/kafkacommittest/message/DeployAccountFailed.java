package com.cwoongc.kafka.kafkacommittest.message;

import lombok.AllArgsConstructor;
import lombok.Builder;
import lombok.Data;
import lombok.NoArgsConstructor;

import java.math.BigDecimal;

@Data
@Builder
@AllArgsConstructor
@NoArgsConstructor
public class DeployAccountFailed {

    private String statusCode;
    private String requestId;
    private String organizationId;
    private String walletId;
    private String coinCode;
    private Fee fee;

    @Data
    @Builder
    @AllArgsConstructor
    @NoArgsConstructor
    public static class Fee {
        private String organizationId;
        private String walletId;
        private String address;
        private BigDecimal estimatedAmount;
        private String coinCode;
    }


}
