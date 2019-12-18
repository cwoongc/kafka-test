package com.cwoongc.kafka.kafkacommittest;

import com.cwoongc.kafka.kafkacommittest.consumer.*;
import com.cwoongc.kafka.kafkacommittest.producer.*;
import com.fasterxml.jackson.annotation.JsonIgnore;
import com.fasterxml.jackson.annotation.JsonInclude;
import com.fasterxml.jackson.annotation.JsonProperty;
import com.fasterxml.jackson.core.JsonGenerator;
import com.fasterxml.jackson.core.JsonParser;
import com.fasterxml.jackson.core.JsonProcessingException;
import com.fasterxml.jackson.databind.*;
import com.fasterxml.jackson.databind.module.SimpleModule;
import com.fasterxml.jackson.module.afterburner.AfterburnerModule;
import lombok.Data;
import lombok.ToString;
import lombok.extern.slf4j.Slf4j;
import org.apache.kafka.common.TopicPartition;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.boot.CommandLineRunner;
import org.springframework.boot.SpringApplication;
import org.springframework.boot.autoconfigure.SpringBootApplication;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Profile;

import java.io.IOException;
import java.math.BigDecimal;
import java.time.Instant;
import java.time.LocalDateTime;
import java.time.ZoneId;
import java.util.Properties;

@Slf4j
@SpringBootApplication
public class KafkaCommitTestApplication {


    public static void main(String[] args) {
        SpringApplication.run(KafkaCommitTestApplication.class, args);
    }

    @Autowired
    private Properties consumerConfig;

    @Autowired
    private Properties producerConfig;


    @Autowired
    private Properties secureConsumerConfig;

    @Autowired
    private Properties secureProducerConfig;



    @Value("${kafka.topic}")
    private String kafkaTopic;


    @Profile({"A"})
    @Bean
    public CommandLineRunner runConsumerA() {
        return args -> {

            ConsumerA consumerA = new ConsumerA(consumerConfig);
            TopicPartition topicPartition = new TopicPartition(kafkaTopic, 0);
            consumerA.start(topicPartition, 65L);
        };
    }

    @Profile({"P"})
    @Bean
    public CommandLineRunner runProducerP() {
        return args -> {

            ProducerP producerP = new ProducerP(producerConfig);
            producerP.start(kafkaTopic, 10);
        };
    }

    @Profile({"C-GeneratedTxCommitter"})
    @Bean
    public CommandLineRunner runGeneratedTxConsumer() {
        return args -> {

            ConsumerGeneratedTxCommitter consumer = new ConsumerGeneratedTxCommitter(secureConsumerConfig);
            TopicPartition topicPartition = new TopicPartition(kafkaTopic, 0);
            consumer.start(topicPartition, 1000000L);
        };
    }

    @Profile({"P-DeployAccountCreated"})
    @Bean
    public CommandLineRunner runProducerGeneratedTxDeployAccountCreated() {
        return args -> {

            ProducerGeneratedTxDeployAccountCreated producer = new ProducerGeneratedTxDeployAccountCreated(secureProducerConfig);
            producer.start(kafkaTopic);
        };
    }

    @Profile({"P-DeployAccountFailed"})
    @Bean
    public CommandLineRunner runProducerGeneratedTxDeployAccountFailed() {
        return args -> {

            ProducerGeneratedTxDeployAccountFailed producer = new ProducerGeneratedTxDeployAccountFailed(secureProducerConfig);
            producer.start(kafkaTopic);
        };
    }

    @Profile({"P-DeployAccountBroadcasted"})
    @Bean
    public CommandLineRunner runProducerGeneratedTxDeployAccountBroadcasted() {
        return args -> {

            ProducerGeneratedTxDeployAccountBroadcasted producer = new ProducerGeneratedTxDeployAccountBroadcasted(secureProducerConfig);
            producer.start(kafkaTopic);
        };
    }

    @Profile({"P-WithdrawalCreated"})
    @Bean
    public CommandLineRunner runProducerGeneratedTxWithdrawalCreated() {
        return args -> {

            ProducerGeneratedTxWithdrawalCreated producer = new ProducerGeneratedTxWithdrawalCreated(secureProducerConfig);
            producer.start(kafkaTopic);
        };
    }



    @Profile({"B"})
    @Bean
    public CommandLineRunner runConsumerB() {
        return args -> {
            ConsumerB consumerB = new ConsumerB(consumerConfig);
            TopicPartition topicPartition = new TopicPartition(kafkaTopic, 0);
            consumerB.start(topicPartition);
        };
    }

    @Profile({"eli-generatedtx-v1"})
    @Bean
    public CommandLineRunner runConsumerEliGeneratedTx() {
        return args -> {
            ConsumerB consumerB = new ConsumerB(secureConsumerConfig);
            TopicPartition topicPartition = new TopicPartition(kafkaTopic, 0);
            consumerB.start(topicPartition);
        };
    }






    @Profile({"seek"})
    @Bean
    public CommandLineRunner runConsumerSeek() {
        return args -> {
            ConsumerSeek consumerSeek = new ConsumerSeek(consumerConfig);
            TopicPartition topicPartition = new TopicPartition(kafkaTopic, 0);
            consumerSeek.start(topicPartition, Long.parseLong(args[0]));
        };
    }

    @Profile({"seekNCommit"})
    @Bean
    public CommandLineRunner runConsumerSeekNCommit() {
        return args -> {
            ConsumerSeekNCommit consumerSeekNCommit = new ConsumerSeekNCommit(consumerConfig);
            TopicPartition topicPartition = new TopicPartition(kafkaTopic, 0);
            consumerSeekNCommit.start(topicPartition, Long.parseLong(args[0]));
        };
    }

    @Profile({"next-record"})
    @Bean
    public CommandLineRunner runConsumerNextRecord() {
        return args -> {
            ConsumerNextRecord consumerNextRecord = new ConsumerNextRecord(consumerConfig);
            TopicPartition topicPartition = new TopicPartition(kafkaTopic, 0);
            consumerNextRecord.start(topicPartition);
        };
    }

    @Profile({"offsets-for-times"})
    @Bean
    public CommandLineRunner runOffsetsForTimes() {
        return args -> {
            ConsumerOffsetsForTimes consumerOffsetsForTimes = new ConsumerOffsetsForTimes(consumerConfig);
            TopicPartition topicPartition = new TopicPartition("lcx-sg-ncash-link-orderbook-v1",0);
            Long timestamp = 1550793600000L;
            consumerOffsetsForTimes.start(topicPartition, timestamp);
        };
    }

    @Profile({"publish"})
    @Bean
    public CommandLineRunner runProducerPublish() {
        return args -> {

            ProducerPublish producerPublish = new ProducerPublish(producerConfig);
            producerPublish.start(kafkaTopic, 1000);
        };
    }

    @Profile({"C1ForRebalancing"})
    @Bean
    public CommandLineRunner runC1ForRebalancingTest() {
        return args -> {
            Consumer1ForRebalancingTest c1 = new Consumer1ForRebalancingTest(consumerConfig);
            c1.start(kafkaTopic);
        };
    }

    @Profile({"C2ForRebalancing"})
    @Bean
    public CommandLineRunner runC2ForRebalancingTest() {
        return args -> {
            Consumer2ForRebalancingTest c2 = new Consumer2ForRebalancingTest(consumerConfig);
            c2.start(kafkaTopic);
        };
    }



    @Profile({"big-decimal"})
    @Bean
    public CommandLineRunner testBigDecimal() {
        return args -> {
            BigDecimal balance = new BigDecimal("100");
            BigDecimal matchedPrice = new BigDecimal("1.125");
            int scale = 8;
            BigDecimal qty = balance.divide(matchedPrice, 5, BigDecimal.ROUND_DOWN);
            log.info("qty : {}",qty);


            BigDecimal amount = BigDecimal.TEN;
            matchedPrice = new BigDecimal("5");
            balance = BigDecimal.ZERO;

            balance = balance.subtract(amount.multiply(matchedPrice));

            log.info("balance: {}", balance);






            ObjectMapper om = new ObjectMapper();
            om.setSerializationInclusion(JsonInclude.Include.NON_NULL);
            om.registerModule(new AfterburnerModule());
            om.configure(DeserializationFeature.FAIL_ON_UNKNOWN_PROPERTIES, false);
            SimpleModule s = new SimpleModule();
            s.addSerializer(LocalDateTime.class, new LocateLocalSerializerTypeHandler());
            s.addDeserializer(LocalDateTime.class, new  LocateLocalDeserializerTypeHandler());
            om.registerModule(s);

            String json = "{\"id\":100000000}";

            VO vo = om.readValue(json, VO.class);

            log.info("vo: {}",om.writeValueAsString(vo));

            json = "{\"id\":100000000,\"balance\":\"32413.44231332\",\"check\":true}";

            vo = om.readValue(json,VO.class);

            log.info("vo: {}",om.writeValueAsString(vo));

            String jsonval = om.writeValueAsString(vo);

            log.info("jsonval: {}",jsonval);


        };
    }

//    @Data
    @ToString
    static class VO {
        @JsonProperty
        Long id;

        @JsonProperty
        String balance;

        @JsonProperty
        boolean check;

        @JsonIgnore
//        @JsonProperty
        public boolean getCheck() {
            return check;
        }

//        @JsonIgnore
        @JsonProperty
        public boolean setCheck(boolean check) {
            return this.check = check;
        }
    }

    static class LocateLocalSerializerTypeHandler extends JsonSerializer<LocalDateTime> {
        public LocateLocalSerializerTypeHandler() {
        }

        public void serialize(LocalDateTime value, JsonGenerator gen, SerializerProvider serializers) throws IOException, JsonProcessingException {

            long l = value.atZone(ZoneId.of("UTC").systemDefault()).toInstant().toEpochMilli();
            gen.writeNumber(l);
        }
    }

    static class LocateLocalDeserializerTypeHandler extends JsonDeserializer<LocalDateTime> {
        public LocateLocalDeserializerTypeHandler() {
        }

        public LocalDateTime deserialize(JsonParser p, DeserializationContext ctxt) throws IOException, JsonProcessingException {
            return LocalDateTime.ofInstant(Instant.ofEpochMilli( p.getValueAsLong() ), ZoneId.of("UTC").systemDefault());
        }
    }









}

