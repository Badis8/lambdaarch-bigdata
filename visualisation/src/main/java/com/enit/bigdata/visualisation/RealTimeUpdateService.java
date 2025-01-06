package com.enit.bigdata.visualisation;

import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.scheduling.annotation.Scheduled;
import org.springframework.stereotype.Service;

import com.fasterxml.jackson.core.JsonProcessingException;

@Service
public class RealTimeUpdateService {
    @Autowired
    private CarLocationRepository repository;

    @Autowired
    private YourEntityUpdateService updateService;

    @Scheduled(fixedRate = 5000)  
    public void fetchAndUpdate() {
        System.out.println("iam sending"); 
        
        repository.findAll().forEach(t -> {
            try {
                updateService.sendUpdate(t);
            } catch (JsonProcessingException e) {
                // TODO Auto-generated catch block
                e.printStackTrace();
            }
        });
    }
}
