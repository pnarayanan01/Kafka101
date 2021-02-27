package com.github.omniprasan;

import org.apache.kafka.clients.producer.KafkaProducer;
import org.apache.kafka.clients.producer.ProducerConfig;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.apache.kafka.common.serialization.StringSerializer;

import java.util.Properties;

public class Producer101 {

    public static void main(String[] args) {

        //create producer properties
        Properties properties = new Properties();
        properties.setProperty(ProducerConfig.BOOTSTRAP_SERVERS_CONFIG, "127.0.0.1:9092");
        properties.setProperty(ProducerConfig.KEY_SERIALIZER_CLASS_CONFIG, StringSerializer.class.getName());
        properties.setProperty(ProducerConfig.VALUE_SERIALIZER_CLASS_CONFIG,StringSerializer.class.getName());

        //create producer
        KafkaProducer<String, String>    producer = new KafkaProducer<String, String>(properties);

        //create producer record
        ProducerRecord<String, String> record1 = new ProducerRecord<String, String>("first_topic", "Hello world");
        //send data
        producer.send(record1);

        //flush data
        producer.flush();

        //close
        producer.close();
    }
}
