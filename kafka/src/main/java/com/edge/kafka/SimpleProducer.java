package com.edge.kafka;

import java.util.*;
import org.apache.kafka.clients.producer.*;
public class SimpleProducer {
  
   public static void main(String[] args) throws Exception{
           
      String topicName = "rpltpc";
	  String key = "Key1";
	  String value = "Value-1";
      
      Properties props = new Properties();
      props.put("bootstrap.servers", "localhost:9092,localhost:9093,localhost:9094");
      props.put("key.serializer","org.apache.kafka.common.serialization.StringSerializer");         
      props.put("value.serializer", "org.apache.kafka.common.serialization.StringSerializer");
	        
      Producer<String, String> producer = new KafkaProducer <>(props);
	
	  ProducerRecord<String, String> record = new ProducerRecord<>(topicName,key,value);
	  System.out.println(producer.send(record));	       
      producer.close();
	  
	  System.out.println("SimpleProducer Completed.");
   }
}