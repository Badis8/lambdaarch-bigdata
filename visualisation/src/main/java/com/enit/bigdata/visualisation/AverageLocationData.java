package com.enit.bigdata.visualisation;

import org.springframework.data.cassandra.core.mapping.PrimaryKey;
import org.springframework.data.cassandra.core.mapping.Table;

@Table("average_location_data")
public class AverageLocationData {
    @PrimaryKey
    private String id;
    private double averageLatitude;
    private double averageLongitude;

    // Getters and Setters
}
