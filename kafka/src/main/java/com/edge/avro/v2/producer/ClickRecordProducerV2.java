package com.edge.avro.v2.producer;

import java.util.*;
import org.apache.kafka.clients.producer.*;

import com.edge.avro.v2.ClickRecord;
public class ClickRecordProducerV2 {

    public static void main(String[] args) throws Exception{

        String topicName = "AvroClicks";
        String msg;

        Properties props = new Properties();
        props.put("bootstrap.servers", "192.168.85.133:9092,192.168.85.133:9093");
        props.put("key.serializer","org.apache.kafka.common.serialization.StringSerializer");
        props.put("value.serializer", "io.confluent.kafka.serializers.KafkaAvroSerializer");
        props.put("schema.registry.url", "http://192.168.85.133:8081");

        Producer<String, ClickRecord> producer = new KafkaProducer <>(props);
        ClickRecord cr = new ClickRecord();
        try{
            cr.setSessionId("10001");
            cr.setChannel("HomePage");
            cr.setIp("192.168.0.1");
            cr.setLanguage("Spanish");
            cr.setOs("Mac");
            cr.setEntryUrl("http://facebook.com/myadd");

            producer.send(new ProducerRecord<String, ClickRecord>(topicName,cr.getSessionId().toString(),cr)).get();

            System.out.println("Complete");
        }
        catch(Exception ex){
            ex.printStackTrace(System.out);
        }
        finally{
            producer.close();
        }

   }
}
