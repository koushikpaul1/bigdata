package com.edge.producer;
// RecordMetadata metadata=producer.send(record).get();
//this will fire the next message only after the get method is success.

import java.util.Properties;

import org.apache.kafka.clients.producer.KafkaProducer;
import org.apache.kafka.clients.producer.Producer;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.apache.kafka.clients.producer.RecordMetadata;

public class BSynchronousProducer {
	public static void main(String[] args) throws Exception{
        
	      String topicName = "edge";
		  String key = "Key1";
		  String value = "Value-1";
	      
	      Properties props = new Properties();
	      props.put("bootstrap.servers", "192.168.85.132:9092");
	      //props.put("bootstrap.servers", "192.168.85.132:9092,192.168.85.132:9093,192.168.85.132:9094");;
	      props.put("key.serializer","org.apache.kafka.common.serialization.StringSerializer");         
	      props.put("value.serializer", "org.apache.kafka.common.serialization.StringSerializer");
		        
	      Producer<String, String> producer = new KafkaProducer <>(props);
		
		  ProducerRecord<String, String> record = new ProducerRecord<>(topicName,key,value);
		  RecordMetadata metadata;
		  try{
	          // metadata = producer.send(record).get();
			  for(int i=0;i<100;i++) {
			  metadata = producer.send(new ProducerRecord<>(topicName,key,value+i)).get();
	           System.out.println("Message is sent to Partition no " + metadata.partition() + " and offset " + metadata.offset());
	           System.out.println("SynchronousProducer Completed with success.");
			  }
	      }catch (Exception e) {
	           e.printStackTrace();
	           System.out.println("SynchronousProducer failed with an exception");
	      }finally{
	           producer.close();
	      }
		  
		  System.out.println("SimpleProducer Completed.");
	   }
}
