package com.eniskeskin.findthetarget;

import java.util.List;

public class CentralUnit {
    private List<String> sensors;
    private List<String> bearings;
    private MyConsumer consumer;

    public CentralUnit(MyConsumer consumer) {
        this.consumer = consumer;
    }
    public void run() {
        consumer.consume();
    }
    public void getSensorLocation (int index) {
        sensors = consumer.getLocation();
        System.out.println("Received message: Location=" + sensors.get(index));
    }
    public void getBearingsLocation (int index) {
        bearings = consumer.getBearings();
        System.out.println("Received message: Bearings=" + bearings.get(index));
    }


}
