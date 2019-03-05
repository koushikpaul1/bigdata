package com.edge.producer;

import java.util.*;
import org.apache.kafka.clients.producer.*;
public class FRandomProducer {
  
   public static void main(String[] args) throws InterruptedException{
           
      String topicName = "edge";
      String msg;
      
      Properties props = new Properties();
      props.put("bootstrap.servers", "192.168.85.132:9092,192.168.85.132:9093,,192.168.85.132:9094");
      props.put("key.serializer","org.apache.kafka.common.serialization.StringSerializer");         
      props.put("value.serializer", "org.apache.kafka.common.serialization.StringSerializer");
            
      Producer<String, String> producer = new KafkaProducer <>(props);
      Random rg = new Random();
      Calendar dt = Calendar.getInstance();
      dt.set(2016,1,1);
      try{
          while(true){
              for (int i=0;i<100;i++){
            	  //Thread.sleep(20);
                msg = dt.get(Calendar.YEAR)+"-"+dt.get(Calendar.MONTH)+"-"+dt.get(Calendar.DATE) + "," + rg.nextInt(1000);
                producer.send(new ProducerRecord<String, String>(topicName,0,"Key",msg)).get();
                msg = dt.get(Calendar.YEAR)+"-"+dt.get(Calendar.MONTH)+"-"+dt.get(Calendar.DATE) + "," + rg.nextInt(1000);
                producer.send(new ProducerRecord<String, String>(topicName,1,"Key",msg)).get();
              }
              dt.add(Calendar.DATE,1);
              System.out.println("Data Sent for " + dt.get(Calendar.YEAR) + "-" + dt.get(Calendar.MONTH) + "-" + dt.get(Calendar.DATE) );
          }
      }
      catch(Exception ex){
          System.out.println("Intrupted");
      }
      finally{
          producer.close();
        }
      
   }
}
