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
public class WithdrawalCreated {

    private String statusCode;
    private String requestId;
    private String coinCode;
    private BigDecimal transferAmount;
    private Fee fee;
    private List<Withdrawal> from;
    private List<Deposit> to;

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

    @Data
    @Builder
    @AllArgsConstructor
    @NoArgsConstructor
    public static class Withdrawal {
        private String organizationId;
        private String walletId;
        private String address;
        private Integer index;
        private BigDecimal amount;
    }

    @Data
    @Builder
    @AllArgsConstructor
    @NoArgsConstructor
    public static class Deposit {
        private String address;
        private String tag;
        private Integer index;
        private BigDecimal amount;
    }
}
