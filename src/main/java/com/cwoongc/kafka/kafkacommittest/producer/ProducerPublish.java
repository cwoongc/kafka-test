package com.cwoongc.kafka.kafkacommittest.producer;

import lombok.extern.slf4j.Slf4j;
import org.apache.kafka.clients.producer.KafkaProducer;
import org.apache.kafka.clients.producer.Producer;
import org.apache.kafka.clients.producer.ProducerRecord;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;
import java.util.Properties;

@Slf4j
public class ProducerPublish {

    private final Properties producerConfig;

    public ProducerPublish(Properties producerConfig) {
        this.producerConfig = producerConfig;
    }

    public void start(String topic, int intervalMillSecond) {

        Producer<String, String> kafkaProducer = new KafkaProducer<>(producerConfig);

//        List<Integer> currentPrices = new ArrayList<>(Arrays.asList(
//            1000 //100개
//            ,1200 //200개
//            ,1500 //300개
//            ,1900 //400개
//            ,800  // 100개
//            ,600 // 200개
//            ,300 // 300개
//            ,10 // 290개
//        ));

        List<Integer> currentPrices = new ArrayList<>(Arrays.asList(
                5001,
                5100 //BUY 100개
                ,5300 // 200
                ,5700 // 400
                ,6500 // 800
                ,8100 // 1600
                ,10000 // 1900
                ,4900 //SELL 100개
                ,4700 //200개
                ,4300 //400개
                ,3500  //800개
                ,1900 // 1600개
                ,1 // 1999개
        ));

        for(int i=0;i<100; i++) {
            currentPrices.addAll(Arrays.asList(
                    5100 //BUY 100개
                    ,5300 // 200
                    ,5700 // 400
                    ,6500 // 800
                    ,8100 // 1600
                    ,10000 // 1900
                    ,4900 //SELL 100개
                    ,4700 //200개
                    ,4300 //400개
                    ,3500  //800개
                    ,1900 // 1600개
                    ,1 // 1999개
            ));
        }


        for(int i=0;i<currentPrices.size();i++) {

            int price = currentPrices.get(i);
            int orderNo = 100000 + i;
            long acceptTime  = 1556167570000L + i;
            long transactionNo = 10000001272322L + i;

            kafkaProducer.send(new ProducerRecord<String, String>(
                topic,
                String.format("{\"userNo\":1,\"orderNo\":%d,\"type\":\"MATCH\",\"buySell\":\"SELL\",\"acceptTime\":%d,\"amount\":\"1\",\"price\":\"%d\",\"matchedUserNo\":2,\"transactionNo\":%d,\"fee\":\"0\",\"orderMatching\":\"LIMIT\",\"makerTaker\":\"MAKER\",\"feeCurrency\":\"MKT\",\"marketCurrencyUsdtPrice\":\"2\"}"
                        ,orderNo
                        ,acceptTime
                        ,price
                        ,transactionNo)
                ),
//                "{\"type\":\"ECHO\"}"),

                (metadata, exception) -> {
                    if (metadata != null) {
                        log.info("[P] Topic: {}, Partition: {}, offset: {}, Price: {}",
                                metadata.topic(),
                                metadata.partition(),
                                metadata.offset(),
                                price
                        );
                    } else {
                        log.error(exception.getMessage(), exception);
                    }
                }
            );

            try {
                Thread.sleep(intervalMillSecond);
            } catch (InterruptedException e) {
                e.printStackTrace();
            }


        }



    }
}
