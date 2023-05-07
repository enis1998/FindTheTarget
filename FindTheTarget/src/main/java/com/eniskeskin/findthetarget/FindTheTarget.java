package com.eniskeskin.findthetarget;

import org.springframework.boot.SpringApplication;
import org.springframework.boot.autoconfigure.SpringBootApplication;

import java.util.ArrayList;
import java.util.List;
import java.util.Random;

@SpringBootApplication

public class FindTheTarget {

	public static void main(String[] args) {
		SpringApplication.run(FindTheTarget.class, args);
		String brokers = "localhost:9092";
		String topic = "my-topic";

		// Creating producer and consumer
		MyProducer producer = new MyProducer(brokers, topic);
		MyConsumer consumer = new MyConsumer(brokers, topic);

		// Sensor location data
		Random rand = new Random();
		int sensor1X = rand.nextInt(500);
		int sensor1Y = rand.nextInt(500);
		Sensor sensor1 = new Sensor(sensor1X, sensor1Y, producer, topic);

		int sensor2X = rand.nextInt(500);
		int sensor2Y = rand.nextInt(500);
		Sensor sensor2 = new Sensor(sensor2X, sensor2Y, producer, topic);

		// Target location data
		int targetX = rand.nextInt(500);
		int targetY = rand.nextInt(500);
		Location target = new Location(targetX, targetY);

		// Sends their location to the Central Unit
		sensor1.sendLocation();
		sensor2.sendLocation();

		// Calculates bearing of target and send calculations to Central Unit
		sensor1.calculateBearings(target);
		sensor2.calculateBearings(target);

		// Closes sending data
		sensor1.close();
		sensor2.close();

		// Creates central unit and consume values
		CentralUnit centralUnit = new CentralUnit(consumer);
		// Starts consumer and print Sensor locations and target location
		centralUnit.run();

		//centralUnit.getSensorLocation(0);
		//centralUnit.getSensorLocation(1);

		//centralUnit.getBearingsLocation(0);
		//centralUnit.getBearingsLocation(1);

	}

}
