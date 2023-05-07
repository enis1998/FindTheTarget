package com.eniskeskin.findthetarget;

import org.apache.kafka.clients.producer.KafkaProducer;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.springframework.context.annotation.Bean;

import java.util.Properties;

public class MyProducer {
    private final KafkaProducer<String, String> producer;
    private final String topic;

    public MyProducer(String brokers, String topic) {
        Properties props = new Properties();
        props.put("bootstrap.servers", brokers);
        props.put("client.id", "MyProducer");
        props.put("key.serializer", "org.apache.kafka.common.serialization.StringSerializer");
        props.put("value.serializer", "org.apache.kafka.common.serialization.StringSerializer");
        this.producer = new KafkaProducer<>(props);
        this.topic = topic;
    }
    @Bean
    public void send(String key, String value) {
        ProducerRecord<String, String> record = new ProducerRecord<>(this.topic, key, value);
        producer.send(record);
    }

    public KafkaProducer<String, String> getProducer() {
        return producer;
    }
}

