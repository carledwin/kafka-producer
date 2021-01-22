package com.carledwinti.producer;

import org.apache.kafka.clients.consumer.ConsumerRecords;
import org.apache.kafka.clients.producer.ProducerRecord;

import java.util.Arrays;
import java.util.List;

public class ConsumeKafka {

    public static void main(String[] args) {

        List<String> topics = Arrays.asList("teste-java");
        ConsumerRecords<String, String> consumerRecords = KafkaConsumerClient.consume(topics);

        if(consumerRecords != null && consumerRecords.iterator().hasNext()){
            consumerRecords.iterator().forEachRemaining(record -> {
                System.out.println("Message: " + record.value());
            });
        }

    }
}
