package com.eniskeskin.findthetarget;

import lombok.Getter;
import org.apache.kafka.clients.consumer.ConsumerRecords;
import org.apache.kafka.clients.consumer.KafkaConsumer;
import org.apache.kafka.common.serialization.StringDeserializer;
import org.springframework.context.annotation.Bean;

import java.util.ArrayList;
import java.util.Collections;
import java.util.List;
import java.util.Properties;

@Getter
public class MyConsumer {
    private ArrayList<Double> sensorX;
    private ArrayList<Double> sensorY;
    private ArrayList<Double> angle;
    private final KafkaConsumer<String, String> consumer;
    private List<String> location;
    private List<String> bearings;
    private final String topic;

    public MyConsumer(String brokers, String topic) {
        Properties props = new Properties();
        props.put("bootstrap.servers", brokers);
        props.put("group.id", "MyConsumer");
        props.put("key.deserializer", StringDeserializer.class.getName());
        props.put("value.deserializer", StringDeserializer.class.getName());
        this.consumer = new KafkaConsumer<>(props);
        this.topic = topic;
        sensorX = new ArrayList<Double>();
        sensorY = new ArrayList<Double>();
        angle = new ArrayList<Double>();
    }

    @Bean
    public void consume() {
        consumer.subscribe(Collections.singletonList(this.topic));

        while (true) {
            ConsumerRecords<String, String> records = consumer.poll(1000);
            records.forEach(record -> {
                if (record.key().equals("sensor")) {
                    //bearings.add(record.value().toString());
                    String[] parts = record.value().split(",");
                    sensorX.add(Double.parseDouble(parts[0]));
                    sensorY.add(Double.parseDouble(parts[1]));

                    System.out.println("Sensor Location " + sensorX.get(0) + " "+ sensorY.get(0));
                }
                else {
                    angle.add(Double.parseDouble(record.value().toString()));
                    //System.out.println("Target Location:" + record.value());
                    //location.add(record.value().toString());
                }
                //System.out.println("Received message: key=" + record.key() + ", value=" + record.value());
            });
            if (!records.isEmpty()) {
                double xValue = sensorX.get(0) + angle.get(0) * Math.cos(angle.get(0));
                double yValue = sensorY.get(0) + angle.get(0) * Math.sin(angle.get(0));
                double xValue2 = sensorX.get(1) + angle.get(1) * Math.cos(angle.get(1));
                double yValue2 = sensorY.get(1) + angle.get(1) * Math.sin(angle.get(1));
                double locationX = (xValue + xValue2) / 2;
                double locationY = (yValue + yValue2) / 2;
                System.out.println("Target Location:" + locationX + "," + locationY);
            }

        }
    }
    public List<String> getLocation() {
        return this.location;
    }
    public List<String> getBearings() {
        return this.bearings;
    }
}
