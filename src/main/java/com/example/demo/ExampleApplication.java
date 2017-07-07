package com.example.demo;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.boot.SpringApplication;
import org.springframework.boot.autoconfigure.SpringBootApplication;

import java.util.Arrays;

@SpringBootApplication
public class ExampleApplication {

	private static final Logger LOG = LoggerFactory.getLogger(ExampleApplication.class);

	public static void main(String[] args) {
		LOG.info("CmdLine Args {}", Arrays.asList(args));
		SpringApplication.run(ExampleApplication.class, args);
	}
}
