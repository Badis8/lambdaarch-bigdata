package tn.enit.bigdata.entity;


import java.io.Serializable;

public class AverageData implements Serializable {

    private String id;
    private double averagelatitude;
    private double averagelongitude;

    public AverageData() {
    }

    public AverageData(String carid, double averageLatitude, double averageLongitude) {
        this.id = carid;
        this.averagelatitude = averageLatitude;
        this.averagelongitude = averageLongitude;
    }

    public String getId() {
        return id;
    }

    public double getAveragelatitude() {
        return averagelatitude;
    }

    public double getAveragelongitude() {
        return averagelongitude;
    }

    @Override
    public String toString() {
        return "AverageData{" +
                "carid='" + id + '\'' +
                ", averageLatitude=" + averagelatitude +
                ", averageLongitude=" + averagelongitude +
                '}';
    }
}
