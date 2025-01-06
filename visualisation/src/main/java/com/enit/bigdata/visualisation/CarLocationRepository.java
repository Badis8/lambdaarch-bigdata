package com.enit.bigdata.visualisation;

import org.springframework.data.cassandra.repository.CassandraRepository;
import java.util.List;

public interface CarLocationRepository extends CassandraRepository<CarLocation, String> {
    List<CarLocation> findByCarId(String carId);
}
