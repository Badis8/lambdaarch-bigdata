package com.enit.bigdata.visualisation;

import org.springframework.boot.SpringApplication;
import org.springframework.boot.autoconfigure.SpringBootApplication;
import org.springframework.scheduling.annotation.EnableScheduling;

@SpringBootApplication 
@EnableScheduling
public class VisualisationApplication {

	public static void main(String[] args) {
		SpringApplication.run(VisualisationApplication.class, args);
	}

}
