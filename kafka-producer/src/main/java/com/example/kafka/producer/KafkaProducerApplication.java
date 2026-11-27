package com.example.kafka.producer;

import org.springframework.boot.SpringApplication;
import org.springframework.boot.autoconfigure.SpringBootApplication;

@SpringBootApplication
public class KafkaProducerApplication {

    public static void main(String[] args) {
        System.exit(SpringApplication.exit(SpringApplication.run(KafkaProducerApplication.class, args)));
    }
}
