package com.eniskeskin.findthetarget;

import java.util.*;
import org.apache.kafka.clients.producer.*;
import org.apache.kafka.clients.consumer.*;
import org.apache.kafka.common.serialization.*;

public class FindTheTargetApplication {
    public static void main(String[] args) throws Exception {

        // Kafka konfigürasyon ayarları
        String sensorTopic = "sensor-verileri";
        String merkeziBirimTopic = "merkezi-birim-verileri";
        String bootstrapServers = "localhost:9092";
        String groupId = "hedefi-bul-grup";

        // Kafka prodücer ayarları
        Properties prodProps = new Properties();
        prodProps.put("bootstrap.servers", bootstrapServers);
        prodProps.put("key.serializer", StringSerializer.class.getName());
        prodProps.put("value.serializer", StringSerializer.class.getName());
        Producer<String, String> prod = new KafkaProducer<>(prodProps);

        // Kafka consumer ayarları
        Properties consProps = new Properties();
        consProps.put("bootstrap.servers", bootstrapServers);
        consProps.put("group.id", groupId);
        consProps.put("key.deserializer", StringDeserializer.class.getName());
        consProps.put("value.deserializer", StringDeserializer.class.getName());
        Consumer<String, String> cons = new KafkaConsumer<>(consProps);
        cons.subscribe(Arrays.asList(sensorTopic));

        // Sensor verileri üretimi
        Random rand = new Random();
        int sensor1X = rand.nextInt(1000);
        int sensor1Y = rand.nextInt(1000);
        int sensor2X = rand.nextInt(1000);
        int sensor2Y = rand.nextInt(1000);
        String sensor1Data = "Sensor1:" + sensor1X + "," + sensor1Y;
        String sensor2Data = "Sensor2:" + sensor2X + "," + sensor2Y;

        // Sensor verilerini Kafka'ya gönderme
        prod.send(new ProducerRecord<>(sensorTopic, "sensor1", sensor1Data));
        prod.send(new ProducerRecord<>(sensorTopic, "sensor2", sensor2Data));

        // Merkezi birim verilerini işleme
        int hedefX = rand.nextInt(500);
        int hedefY = rand.nextInt(500);
        double sensor1Kerteriz = Math.atan2(hedefY-sensor1Y, hedefX-sensor1X);
        double sensor2Kerteriz = Math.atan2(hedefY-sensor2Y, hedefX-sensor2X);
        String merkeziBirimData = "Hedef:" + hedefX + "," + hedefY + " Sensor1Kerteriz:" + sensor1Kerteriz + " Sensor2Kerteriz:" + sensor2Kerteriz;

        // Merkezi birim verilerini Kafka'ya gönderme
        prod.send(new ProducerRecord<>(merkeziBirimTopic, merkeziBirimData));

        // Merkezi birim verilerini tüketme
        cons.subscribe(Arrays.asList(merkeziBirimTopic));
        while (true) {
            ConsumerRecords<String, String> records = cons.poll(100);
            for (ConsumerRecord<String, String> record : records) {
                String value = record.value();
                // Hedef koordinatları ve sensör kerterizleri ayrıştırma
                String[] parts = value.split(" ");
                String[] hedefParts = parts[0].split(",");
                 hedefX = Integer.parseInt(hedefParts[0].substring(6));
                 hedefY = Integer.parseInt(hedefParts[1]);
                String[] sensor1Parts = parts[1].split(":");
                 sensor1Kerteriz = Double.parseDouble(sensor1Parts[1]);
                String[] sensor2Parts = parts[2].split(":");
                 sensor2Kerteriz = Double.parseDouble(sensor2Parts[1]);
                // Hedef konumunu hesaplama
                double x1 = hedefX + Math.cos(sensor1Kerteriz);
                double y1 = hedefY + Math.sin(sensor1Kerteriz);
                double x2 = hedefX + Math.cos(sensor2Kerteriz);
                double y2 = hedefY + Math.sin(sensor2Kerteriz);
                double hedefKonumX = (x1 + x2) / 2;
                double hedefKonumY = (y1 + y2) / 2;
                // Hedef konumunu yazdırma
                System.out.println("Hedef konumu: " + hedefKonumX + "," + hedefKonumY);
            }
        }
    }
}