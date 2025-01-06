package com.enit.bigdata.visualisation;

import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.messaging.simp.SimpMessagingTemplate;
import org.springframework.stereotype.Service;

import com.fasterxml.jackson.core.JsonProcessingException;
import com.fasterxml.jackson.databind.ObjectMapper;

@Service
public class YourEntityUpdateService {
    @Autowired
    private SimpMessagingTemplate messagingTemplate;
    @Autowired
    private ObjectMapper objectMapper;
    public void sendUpdate(CarLocation entity) throws JsonProcessingException { 
        String jsonString = objectMapper.writeValueAsString(entity);
        messagingTemplate.convertAndSend("/topic/updates", jsonString);
    }
}

