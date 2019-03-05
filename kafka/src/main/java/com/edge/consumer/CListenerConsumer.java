package com.edge.consumer;
//With Listener this class keeps the current(processed) offset in case of a rebalance occurs in between processing. 

//Create a topic<edge> with two partition.
//This consumer belongs to a group, launch second instance of the same class, rebalance will trigger and instead of two, now each of them will have 
//a single partition to consume from. If launched third instance, again rebalance will occur and only two of the will point to a partition each.

import java.util.*;
import org.apache.kafka.clients.consumer.*;

import com.edge.beanUtil.RebalanceListner;

public class CListenerConsumer{
    
    
    public static void main(String[] args) throws Exception{

            String topicName = "edge";
            KafkaConsumer<String, String> consumer = null;
            
            String groupName = "RG";
            Properties props = new Properties();
            props.put("bootstrap.servers", "192.168.85.132:9092,192.168.85.132:9093");
            props.put("group.id", groupName);
            props.put("key.deserializer", "org.apache.kafka.common.serialization.StringDeserializer");
            props.put("value.deserializer", "org.apache.kafka.common.serialization.StringDeserializer");
            props.put("enable.auto.commit", "false");

            consumer = new KafkaConsumer<>(props);
            RebalanceListner rebalanceListner = new RebalanceListner(consumer);// Listener , to keep track of the processed offsets.            
            consumer.subscribe(Arrays.asList(topicName),rebalanceListner);//passing the listener,the parent interface of Listener has a method onPartitionRevoked,
            //when partition is revoked this method gets called automatically, within this method we have to implement the commiting of the offset.  so new reads happens from this offset
            try{
                while (true){
                    ConsumerRecords<String, String> records = consumer.poll(100);
                    for (ConsumerRecord<String, String> record : records){
                        //System.out.println("Topic:"+ record.topic() +" Partition:" + record.partition() + " Offset:" + record.offset() + " Value:"+ record.value());
                       // Do some processing and save it to Database
                        rebalanceListner.addOffset(record.topic(), record.partition(),record.offset());// update the processed offset
                    }
                        consumer.commitSync(rebalanceListner.getCurrentOffsets());//manual sync in normal case( no rebalance)
                }
            }catch(Exception ex){
                System.out.println("Exception.");
                ex.printStackTrace();
            }
            finally{
                    consumer.close();
            }
    }
    
}
