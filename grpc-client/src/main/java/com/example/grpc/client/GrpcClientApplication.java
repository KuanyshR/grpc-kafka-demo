package com.example.grpc.client;

import org.springframework.boot.SpringApplication;
import org.springframework.boot.autoconfigure.SpringBootApplication;

@SpringBootApplication
public class GrpcClientApplication {

    public static void main(String[] args) {
        // Run the client once, then exit with the proper exit code
        System.exit(SpringApplication.exit(SpringApplication.run(GrpcClientApplication.class, args)));
    }
}
