package com.carledwinti.producer;

import org.apache.kafka.clients.consumer.ConsumerConfig;
import org.apache.kafka.clients.consumer.ConsumerRecords;
import org.apache.kafka.clients.consumer.KafkaConsumer;
import org.apache.kafka.common.serialization.StringDeserializer;

import java.time.Duration;
import java.util.Arrays;
import java.util.List;
import java.util.Properties;

public class ConsumeKafkaLooping {

    public static void main(String[] args) {

        //cmd --> kafka-console-producer.bat --broker-list localhost:9092,localhost:9093 --topic teste-java

        Properties properties = new Properties();
        properties.put(ConsumerConfig.BOOTSTRAP_SERVERS_CONFIG, "localhost:9092,localhost:9093");
        properties.put(ConsumerConfig.KEY_DESERIALIZER_CLASS_CONFIG, StringDeserializer.class.getName());
        properties.put(ConsumerConfig.VALUE_DESERIALIZER_CLASS_CONFIG, StringDeserializer.class.getName());
        properties.put(ConsumerConfig.GROUP_ID_CONFIG, "grupojava1");
        properties.put(ConsumerConfig.AUTO_OFFSET_RESET_CONFIG, "earliest");//para pegar mensagens desde o início passamos mais esse parâmetro

        try(KafkaConsumer<String, String> kafkaConsumer = new KafkaConsumer<>(properties)){
            List<String> topics = Arrays.asList("teste-java");
            kafkaConsumer.subscribe(topics);
            //kafkaConsumer.close(); será executado automaticamente pelo Java
            System.out.println("Iniciando listening");

            while (true){
                ConsumerRecords<String, String> consumerRecords = kafkaConsumer.poll(Duration.ofSeconds(30));

                if (consumerRecords != null && consumerRecords.iterator().hasNext()) {
                    consumerRecords.iterator().forEachRemaining(record -> {
                        System.out.println("Message: " + record.value());
                    });
                }
            }
        }catch(Exception e){
            System.out.println(e.getMessage());
        }
    }
}
