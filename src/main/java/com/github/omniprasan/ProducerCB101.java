package com.github.omniprasan;

import org.apache.kafka.clients.producer.*;
import org.apache.kafka.common.serialization.StringSerializer;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.Properties;

public class ProducerCB101 {

    public static void main(String[] args) {

        Logger logger = LoggerFactory.getLogger(ProducerCB101.class.getName());

        //create producer properties
        Properties properties = new Properties();
        properties.setProperty(ProducerConfig.BOOTSTRAP_SERVERS_CONFIG, "127.0.0.1:9092");
        properties.setProperty(ProducerConfig.KEY_SERIALIZER_CLASS_CONFIG, StringSerializer.class.getName());
        properties.setProperty(ProducerConfig.VALUE_SERIALIZER_CLASS_CONFIG,StringSerializer.class.getName());

        //create producer
        KafkaProducer<String, String>    producer = new KafkaProducer<>(properties);

        //on a loop
        for (int i=0; i<10; i++)
        {

            //create producer record
            ProducerRecord<String, String> record1 = new ProducerRecord<>("first_topic", "Hello world " + i);
            //send data async
            producer.send(record1, (recordMetadata, e) -> {
                if (e == null) {
                    logger.info("Received new metadata \n" +
                            "Topic: " + recordMetadata.topic() + "\n" +
                            "Partition: " + recordMetadata.partition() + "\n" +
                            "Offset: " + recordMetadata.offset() + "\n" +
                            "Timestamp: " + recordMetadata.timestamp());
                } else {
                    logger.error("Error while producing" + e);
                }
            });
        }
        //flush data
        producer.flush();

        //close
        producer.close();
    }
}
