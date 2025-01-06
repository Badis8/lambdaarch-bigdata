package com.enit.project;


import java.util.Date;

import java.util.Properties;
import java.util.Random;
import java.util.UUID;

import kafka.javaapi.producer.Producer;
import kafka.producer.KeyedMessage;
import kafka.producer.ProducerConfig;


public class CarLocationProducer {

    private final Producer<String, CarLocation> producer;
    private final Random rand = new Random();
    private final String[] carIds = {"CAR001", "CAR002", "CAR003"};
    private final double[][] carCoordinates = {
        {36.8065, 10.1815}, // CAR001 initial coordinates (example: Tunis, Tunisia)
        {37.7749, -122.4194}, // CAR002 initial coordinates (example: San Francisco, USA)
        {48.8566, 2.3522}  // CAR003 initial coordinates (example: Paris, France)
    };
    public CarLocationProducer(final Producer<String, CarLocation> producer) {
        this.producer = producer;
    }

    public static void main(String[] args) throws Exception {

        Properties properties = PropertyFileReader.readPropertyFile();
        Producer<String, CarLocation> producer = new Producer<>(new ProducerConfig(properties));
        CarLocationProducer iotProducer = new CarLocationProducer(producer);
        iotProducer.generateIoTEvent(properties.getProperty("kafka.topic"));
    }

    private void generateIoTEvent(String topic) throws InterruptedException {
        while (true) {
            for (int i = 0; i < carIds.length; i++) {
                CarLocation event = generateCarLocationData(i);
                System.out.println("Sent: " + event);
                producer.send(new KeyedMessage<>(topic, event));
            }
            Thread.sleep(rand.nextInt(5000 - 2000) + 2000); // random delay of 2 to 5 seconds
        }
    }

    private CarLocation generateCarLocationData(int carIndex) {
        String carId = carIds[carIndex];
        double baseLatitude = carCoordinates[carIndex][0];
        double baseLongitude = carCoordinates[carIndex][1];

        // Generate a random movement within a 0.01° range
        double randomLatitudeShift = (rand.nextDouble() - 0.5) * 0.02;
        double randomLongitudeShift = (rand.nextDouble() - 0.5) * 0.02;

        double newLatitude = baseLatitude + randomLatitudeShift;
        double newLongitude = baseLongitude + randomLongitudeShift;

        return new CarLocation(carId, newLatitude, newLongitude, new Date());
    }
}
 
