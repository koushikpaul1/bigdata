package com.edge.producer;
import java.util.*;
import org.apache.kafka.clients.producer.*;
public class AFireNForgetProducer {
  
   public static void main(String[] args) throws Exception{
           
      String topicName = "c-edge";
	  String key = "Key1";
	  String value = "Value-1";
      
      Properties props = new Properties();
      props.put("bootstrap.servers", "192.168.85.132:9092");
      //props.put("bootstrap.servers", "192.168.85.132:9092,192.168.85.132:9093,192.168.85.132:9094");
      props.put("key.serializer","org.apache.kafka.common.serialization.StringSerializer");         
      props.put("value.serializer", "org.apache.kafka.common.serialization.StringSerializer");
	        
      Producer<String, String> producer = new KafkaProducer <>(props);
	
	  ProducerRecord<String, String> record = new ProducerRecord<>(topicName,key,value);
	  producer.send(record);	       
      producer.close();
	  
	  System.out.println("SimpleProducer Completed.");
   }
}