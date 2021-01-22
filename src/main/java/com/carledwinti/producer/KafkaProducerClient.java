package com.carledwinti.producer;

import org.apache.kafka.clients.producer.KafkaProducer;
import org.apache.kafka.clients.producer.ProducerConfig;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.apache.kafka.common.serialization.StringSerializer;

import java.util.Properties;

public class KafkaProducerClient {

    public static void send(ProducerRecord<String, String> producerRecord){

        Properties properties = new Properties();
        properties.put(ProducerConfig.BOOTSTRAP_SERVERS_CONFIG, "localhost:9092,localhost:9093");
        properties.put(ProducerConfig.KEY_SERIALIZER_CLASS_CONFIG, StringSerializer.class.getName());
        properties.put(ProducerConfig.VALUE_SERIALIZER_CLASS_CONFIG, StringSerializer.class.getName());

       /*
        KafkaProducer<String, String> kafkaProducer = new KafkaProducer<>(properties);
        kafkaProducer.send(producerRecord);
        kafkaProducer.close();
        */

        //try with resources // fecha automaticamente o kafkaProducer
        try(KafkaProducer<String, String> kafkaProducer = new KafkaProducer<>(properties)){
            kafkaProducer.send(producerRecord);
            //kafkaProducer.close(); será executado automaticamente pelo Java
            System.out.println("Mensagem enviada com sucesso");
        }catch(Exception e){
            System.out.println(e.getMessage());
        }
    }
}
