package com.example.demo.consumers;

import org.apache.kafka.clients.consumer.KafkaConsumer;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.boot.CommandLineRunner;
import org.springframework.stereotype.Component;

import java.util.ArrayList;
import java.util.List;
import java.util.Properties;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.stream.Collectors;
import java.util.stream.IntStream;

/**
 * Created by jpairaiturkar on 7/7/17.
 */
@Component
public class Driver implements CommandLineRunner {

    private static final Logger LOG = LoggerFactory.getLogger(Driver.class);


    @Value("${consumer.groupid}")
    private  String groupId  ;

    @Value("${kafka.consumer.topic}")
    private  String topic ;

    @Value("${kafka.brokers}")
    private  String brokers ;

    @Value("${consumer.nthreads}")
    private  Integer nThreads ;


    private ExecutorService executor;

    private  KafkaConsumer<String, String> consumer;

    private List<ExampleConsumer> consumers;

    public void run(String... args) {

        Properties prop = ExampleConsumer.createConsumerConfig(brokers, groupId);

        this.consumer = new KafkaConsumer<>(prop);
        this.consumers = new ArrayList<ExampleConsumer>();

        Integer  nPartitions = this.consumer.partitionsFor(this.topic).size();
        Integer nPartitionsPerThread = nPartitions/nThreads;
        Integer thisAssignment  ;


        this.executor = Executors.newFixedThreadPool(2);//creating a pool of 5 threads


        for ( int i =0; i< nThreads; i++) {


            if (i == (nThreads -1)) {
                // handle odd nos
                thisAssignment =  nPartitions - (i-1)*nPartitionsPerThread;
            } else {
                thisAssignment = nPartitionsPerThread;
            }

            List<Integer> range =  IntStream.rangeClosed(
                    i*nPartitionsPerThread, i*nPartitionsPerThread + thisAssignment - 1)
                    .boxed().collect(Collectors.toList());

            LOG.info("Parition Range  {}", range);


            ExampleConsumer cons = new ExampleConsumer(
                    String.format("Consumer%d",i),
                    brokers,
                    groupId,
                    topic,
                    range);
            consumers.add(cons);
            this.executor.execute(cons);

            nPartitions -=  nPartitionsPerThread;
        }
    }

}
