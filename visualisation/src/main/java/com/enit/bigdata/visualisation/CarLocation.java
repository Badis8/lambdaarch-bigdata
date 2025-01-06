package com.enit.bigdata.visualisation;

import org.springframework.data.cassandra.core.mapping.PrimaryKey;
import org.springframework.data.cassandra.core.mapping.Table;
import java.time.Instant;

@Table("live")
public class CarLocation {
    @PrimaryKey
    private String carId;
    @Override
    public String toString() {
        return "CarLocation [carId=" + carId + ", latitude=" + latitude + ", longitude=" + longitude + ", timestamp="
                + timestamp + ", getCarId()=" + getCarId() + ", getLatitude()=" + getLatitude() + ", getLongitude()="
                + getLongitude() + ", getTimestamp()=" + getTimestamp() + ", getClass()=" + getClass() + ", hashCode()="
                + hashCode() + ", toString()=" + super.toString() + "]";
    }
    public void setCarId(String carId) {
        this.carId = carId;
    }
    public void setLatitude(double latitude) {
        this.latitude = latitude;
    }
    public void setLongitude(double longitude) {
        this.longitude = longitude;
    }
    public void setTimestamp(Instant timestamp) {
        this.timestamp = timestamp;
    }
    private double latitude;
    private double longitude;
    private Instant timestamp;
    public String getCarId() {
        return carId;
    }
    public double getLatitude() {
        return latitude;
    }
    public double getLongitude() {
        return longitude;
    }
    public Instant getTimestamp() {
        return timestamp;
    }

     
}
