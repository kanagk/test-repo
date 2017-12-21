package com.reancloud.kanag;

import org.springframework.boot.SpringApplication;
import org.springframework.boot.autoconfigure.SpringBootApplication;
import org.springframework.context.annotation.ComponentScan;

@SpringBootApplication
@ComponentScan("com.reancloud.kanag")
public class RCServiceApplication {

	public static void main(String[] args) {
		SpringApplication.run(RCServiceApplication.class, args);
	}
}
