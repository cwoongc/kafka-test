package com.cwoongc.kafka.kafkacommittest.config;

import lombok.Getter;
import org.apache.kafka.clients.consumer.ConsumerConfig;
import org.apache.kafka.clients.producer.ProducerConfig;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;

import java.util.Properties;

@Configuration
@Getter
public class KafkaConfiguration {

    public static final String DESERIALIZER = "org.apache.kafka.common.serialization.StringDeserializer";
    public static final String SERIALIZER = "org.apache.kafka.common.serialization.StringSerializer";

    //AUTO_OFFSET_RESET_CONFIG
    public static final String LATEST = "latest"; // default
    public static final String EARLIEST = "earliest";
    public static final String  NONE = "none";

    @Value("${kafka.bootstrap-servers}")
    private String bootstrapServers;

    @Value("${kafka.consumer.group-id}")
    private String consumerGroupId;

    @Bean
    public Properties consumerConfig() {
        final Properties props = new Properties();

        props.put(ConsumerConfig.BOOTSTRAP_SERVERS_CONFIG, bootstrapServers);
        props.put(ConsumerConfig.GROUP_ID_CONFIG, consumerGroupId);
        props.put(ConsumerConfig.ENABLE_AUTO_COMMIT_CONFIG, "false");
        //카프카에 본컨슈머의 offset이 expire되거나/커밋된게 하니도없거나 해서 없을경우 맨앞에서 부터 읽어올까, 지금부터 들어오는것부터 읽을까 선택
        props.put(ConsumerConfig.AUTO_OFFSET_RESET_CONFIG, LATEST);
        props.put(ConsumerConfig.KEY_DESERIALIZER_CLASS_CONFIG, DESERIALIZER);
        props.put(ConsumerConfig.VALUE_DESERIALIZER_CLASS_CONFIG, DESERIALIZER);

        return props;
    }

    @Bean
    public Properties producerConfig() {
        final Properties props = new Properties();

        props.put(ProducerConfig.BOOTSTRAP_SERVERS_CONFIG, bootstrapServers);
        props.put(ProducerConfig.ACKS_CONFIG, "all");
        props.put(ProducerConfig.COMPRESSION_TYPE_CONFIG, "gzip");
        props.put(ProducerConfig.KEY_SERIALIZER_CLASS_CONFIG, SERIALIZER);
        props.put(ProducerConfig.VALUE_SERIALIZER_CLASS_CONFIG, SERIALIZER);

        return  props;
    }

}
