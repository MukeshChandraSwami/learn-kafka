package com.learn.learnkafka;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.boot.SpringApplication;
import org.springframework.boot.autoconfigure.SpringBootApplication;
import org.springframework.context.annotation.ComponentScan;

@SpringBootApplication
@ComponentScan("com.learn")
public class LearnKafkaApplication {

	private static Logger log = LoggerFactory.getLogger(LearnKafkaApplication.class);

	public static void main(String[] args) {

		SpringApplication.run(LearnKafkaApplication.class, args);
	}
}
