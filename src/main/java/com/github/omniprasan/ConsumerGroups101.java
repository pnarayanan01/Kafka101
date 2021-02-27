package com.github.omniprasan;

import org.apache.kafka.clients.consumer.ConsumerConfig;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.kafka.clients.consumer.ConsumerRecords;
import org.apache.kafka.clients.consumer.KafkaConsumer;
import org.apache.kafka.common.serialization.StringDeserializer;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.time.Duration;
import java.util.Collections;
import java.util.Properties;

public class ConsumerGroups101 {


    public static void main(String[] args) {
        Logger logger = LoggerFactory.getLogger(ConsumerGroups101.class.getName());
        Properties properties = new Properties();
        String bootstrap = "127.0.0.1:9092";
        String groupId = "java-ConsumerGroups101";
        String topic = "first_topic";
        Duration duration = Duration.ofMillis(100);

        properties.setProperty(ConsumerConfig.BOOTSTRAP_SERVERS_CONFIG, bootstrap);
        properties.setProperty(ConsumerConfig.KEY_DESERIALIZER_CLASS_CONFIG, StringDeserializer.class.getName());
        properties.setProperty(ConsumerConfig.VALUE_DESERIALIZER_CLASS_CONFIG, StringDeserializer.class.getName());
        properties.setProperty(ConsumerConfig.GROUP_ID_CONFIG, groupId);
        properties.setProperty(ConsumerConfig.AUTO_OFFSET_RESET_CONFIG, "earliest");

        //create Consumer
        KafkaConsumer<String, String> consumer = new KafkaConsumer<String, String>(properties);
        //Subscribe Consumer

        consumer.subscribe(Collections.singleton(topic));
        // Poll Data

        while (true) {
            ConsumerRecords<String, String> records = consumer.poll(duration);
            for (ConsumerRecord<String, String> record : records
            ) {
                logger.info("Key: "+ record.key()+ " Value: "+record.value());
                logger.info("partition: "+ record.partition()+ " offset: "+record.offset());

            }
        }
    }
}
