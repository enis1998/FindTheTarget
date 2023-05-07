package com.eniskeskin.findthetarget;

import lombok.Getter;
import lombok.Setter;
import org.apache.kafka.clients.producer.KafkaProducer;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.springframework.context.annotation.Bean;

@Getter@Setter
public class Sensor extends Location{
    private String topic;
    private final String key = "sensor";

    private MyProducer producer;

    public Sensor(int x, int y, MyProducer producer, String topic) {
        super(x, y);
        this.topic = topic;
        this.producer = producer;
    }
    public void calculateBearings(Location target) {
        // Sensör hedefi tespit ettiğinde, hedefin kerteriz bilgilerini hesaplayın
        double[] vector1 = {target.getX() - this.getX(), target.getY() - this.getY()};
        double[] vector2 = {1, 0};

        // dot product
        double dotProduct = vector1[0] * vector2[0] + vector1[1] * vector2[1];

        // Vektör uzunluklarını hesapla
        double vector1Length = Math.sqrt(Math.pow(vector1[0], 2) + Math.pow(vector1[1], 2));
        double vector2Length = Math.sqrt(Math.pow(vector2[0], 2) + Math.pow(vector2[1], 2));

        // Açıyı hesapla
        double angle = Math.acos(dotProduct / (vector1Length * vector2Length));
        //double sensorBearings = Math.atan2(target.getY()-this.getY(), target.getX()-this.getX());
        producer.send("bearings", ""+Math.toDegrees(angle));
    }
    @Bean
    public void sendLocation() {
        String message = String.format("%d,%d", this.getX(), this.getY());
        producer.send(key, message);
    }
    public void close() {
        producer.getProducer().close();
    }

}
