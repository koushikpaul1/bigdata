package com.edge.producer;

import java.util.*;
import org.apache.kafka.clients.producer.*;
import org.apache.kafka.common.Cluster;
import org.apache.kafka.common.PartitionInfo;
import org.apache.kafka.common.record.InvalidRecordException;
import org.apache.kafka.common.utils.Utils;
public class ECustomPartitionerSensorProducer {

   public static void main(String[] args) throws Exception{

      String topicName = "edge";

      Properties props = new Properties();
      props.put("bootstrap.servers", "192.168.85.133:9092,192.168.85.133:9093");
      props.put("key.serializer","org.apache.kafka.common.serialization.StringSerializer");
      props.put("value.serializer", "org.apache.kafka.common.serialization.StringSerializer");
      props.put("partitioner.class", "SensorPartitioner");
      props.put("speed.sensor.name", "TSS");

      Producer<String, String> producer = new KafkaProducer <>(props);

         for (int i=0 ; i<10 ; i++)
         producer.send(new ProducerRecord<>(topicName,"SSP"+i,"500"+i));

         for (int i=0 ; i<10 ; i++)
         producer.send(new ProducerRecord<>(topicName,"TSS","500"+i));

      producer.close();

          System.out.println("SimpleProducer Completed.");
   }
}



