package com.example.demo.consumers;

import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.kafka.clients.consumer.ConsumerRecords;
import org.apache.kafka.clients.consumer.KafkaConsumer;
import org.apache.kafka.common.TopicPartition;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.ArrayList;
import java.util.List;
import java.util.Properties;

/**
 * Created by jpairaiturkar on 7/7/17.
 */
public class ExampleConsumer implements Runnable {

    private static final Logger LOG = LoggerFactory.getLogger(ExampleConsumer.class);
    private final KafkaConsumer<String, String> consumer;
    private final String topic, name;


    public ExampleConsumer(String name, String brokers, String groupId, String topic, List<Integer> partitions) {
        Properties prop = createConsumerConfig(brokers, groupId);
        this.consumer = new KafkaConsumer<>(prop);
        this.topic = topic;
        this.name = name;


        List<TopicPartition> kafkaPartitions = new ArrayList<TopicPartition>();

        for (int partition : partitions ){
            kafkaPartitions.add(new TopicPartition(topic,partition));

        }

        this.consumer.assign(kafkaPartitions);
    }

    public static Properties createConsumerConfig(String brokers, String groupId) {
        Properties props = new Properties();
        props.put("bootstrap.servers", brokers);
        props.put("group.id", groupId);
        props.put("enable.auto.commit", "true");
        props.put("auto.commit.interval.ms", "1000");
        props.put("session.timeout.ms", "30000");
        props.put("auto.offset.reset", "earliest");
        props.put("key.deserializer", "org.apache.kafka.common.serialization.StringDeserializer");
        props.put("value.deserializer", "org.apache.kafka.common.serialization.StringDeserializer");
        return props;
    }

    @Override
    public void run() {
        while (true) {
            ConsumerRecords<String, String> records = consumer.poll(100);
            for (ConsumerRecord<String, String> record : records) {
                LOG.info("Listerner :" + this.name + " Receive message: " + record.value() + ", Partition: "
                        + record.partition() + ", Offset: " + record.offset() + ", by ThreadID: "
                        + Thread.currentThread().getId());
            }
        }

    }


}

