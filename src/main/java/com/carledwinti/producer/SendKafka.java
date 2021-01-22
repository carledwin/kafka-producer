package com.carledwinti.producer;

import org.apache.kafka.clients.producer.ProducerRecord;

public class SendKafka {

    public static void main(String[] args) {

        //cmd--> kafka-topics --bootstrap-server localhost:9093 --create --topic teste-java
        KafkaProducerClient.send(new ProducerRecord<String, String>("teste-java", "Mensagem 1 enviada via Java"));
    }
}
