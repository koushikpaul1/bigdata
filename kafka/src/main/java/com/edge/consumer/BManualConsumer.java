package com.edge.consumer;

import java.util.*;
import java.io.*;
import org.apache.kafka.clients.consumer.KafkaConsumer;

import com.edge.beanUtil.Supplier;

import org.apache.kafka.clients.consumer.ConsumerRecords;
import org.apache.kafka.clients.consumer.ConsumerRecord;

public class BManualConsumer{

    public static void main(String[] args) throws Exception{


		String topicName = "edge";
		String groupName = "edgeGroup";

		Properties props = new Properties();
		props.put("bootstrap.servers", "192.168.85.132:9092,192.168.85.132:9093");
		props.put("group.id", groupName);
		props.put("key.deserializer", "org.apache.kafka.common.serialization.StringDeserializer");
		props.put("value.deserializer", "com.edge.beanUtil.SupplierDeserializer");

        KafkaConsumer<String, Supplier> consumer = null;

        try {
            consumer = new KafkaConsumer<>(props);
            consumer.subscribe(Arrays.asList(topicName));

            while (true){
                ConsumerRecords<String, Supplier> records = consumer.poll(100);
                for (ConsumerRecord<String, Supplier> record : records){
                	System.out.println("Key= "+String.valueOf(record.key())+" Supplier id= " + String.valueOf(record.value().getID()) + " Supplier  Name = "
    						+ record.value().getName() + " Supplier Start Date = "
    						+ record.value().getStartDate().toString());        }
                consumer.commitAsync();//manual commit before every poll
            }
        }catch(Exception ex){
            ex.printStackTrace();
        }finally{
            consumer.commitSync();//
            consumer.close();
        }
    }
}
