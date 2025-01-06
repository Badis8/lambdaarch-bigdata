package tn.enit.bigdata.entity;
import java.io.Serializable;
import java.util.Date;
 
import com.fasterxml.jackson.annotation.JsonFormat;

public class CarLocation implements Serializable {
    
    private String carid;
    private double latitude;
    private double longitude;
    @JsonFormat(shape = JsonFormat.Shape.STRING, pattern = "yyyy-MM-dd HH:mm:ss", timezone = "MST")
    private Date timestamp;

    public CarLocation() {

    }

    public CarLocation(String carId, double latitude, double longitude, Date timestamp) {
        super();
        this.carid = carId;
        this.latitude = latitude;
        this.longitude = longitude;
        this.timestamp = timestamp;
    }
    public CarLocation(String carId, double latitude, double longitude ) {
        super();
        this.carid = carId;
        this.latitude = latitude;
        this.longitude = longitude;
    
    }
    public String getCarid() {
        return carid;
    }

    public double getLatitude() {
        return latitude;
    }

    public double getLongitude() {
        return longitude;
    }

    public Date getTimestamp() {
        return timestamp;
    }
}
