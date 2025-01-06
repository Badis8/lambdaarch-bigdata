package com.enit.bigdata.visualisation;

import org.springframework.data.cassandra.repository.CassandraRepository;

public interface AverageLocationDataRepository extends CassandraRepository<AverageLocationData, String> {}
