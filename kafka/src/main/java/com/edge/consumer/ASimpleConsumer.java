package com.edge.consumer;

// Simple consumer, polls infinitely 
//props.put("enable.auto.commit", "true");=> default value , it commits every five sec
//this may led to duplicate consume by a different consumer in case of failed consumer and rebalance
import java.util.*;
import org.apache.kafka.clients.consumer.KafkaConsumer;

import com.edge.beanUtil.Supplier;

import org.apache.kafka.clients.consumer.ConsumerRecords;
import org.apache.kafka.clients.consumer.ConsumerRecord;

public class ASimpleConsumer {

	@SuppressWarnings("deprecation")
	public static void main(String[] args) throws Exception {

		String topicName = "locator";
		String groupName = "edgeGroup";

		Properties props = new Properties();
		props.put("bootstrap.servers", "192.168.85.133:9092");
		props.put("group.id", groupName);
		props.put("key.deserializer", "org.apache.kafka.common.serialization.StringDeserializer");
		props.put("value.deserializer", "org.apache.kafka.common.serialization.StringDeserializer");
		props.put("enable.auto.commit", "true");//this is by default
		props.put("auto.commit.interval.ms", "5000");//this is by default
		
		KafkaConsumer<String, Supplier> consumer = new KafkaConsumer<>(props);
		consumer.subscribe(Arrays.asList(topicName));

		while (true) {
			ConsumerRecords<String, Supplier> records = consumer.poll(100);
			for (ConsumerRecord<String, Supplier> record : records) {
				System.out.println("Key= "+String.valueOf(record.key())+" value= " + String.valueOf(record.value())) ;
			}
		}

	}
}