package com.edge.producer;

import java.util.*;
import java.text.DateFormat;
import java.text.SimpleDateFormat;
import org.apache.kafka.clients.producer.*;

import com.edge.beanUtil.Supplier;

public class DSupplierProducer {

	public static void main(String[] args) throws Exception {

		String topicName = "edge";

		Properties props = new Properties();
		props.put("bootstrap.servers", "192.168.85.132:9092,192.168.85.132:9093,,192.168.85.132:9094");
		props.put("key.serializer", "org.apache.kafka.common.serialization.StringSerializer");
		props.put("value.serializer", "com.edge.beanUtil.SupplierSerializer");

		Producer<String, Supplier> producer = new KafkaProducer<>(props);

		DateFormat df = new SimpleDateFormat("yyyy-MM-dd");
		Supplier sp1 = new Supplier(101, "Xyz Pvt Ltd.", df.parse("2016-04-01"));
		Supplier sp2 = new Supplier(102, "Abc Pvt Ltd.", df.parse("2012-01-01"));
		try {int i=0;
			while (true) {Thread.sleep(2000);
				producer.send(new ProducerRecord<String, Supplier>(topicName, "SUP"+ ++i, sp1)).get();
				//producer.send(new ProducerRecord<String, Supplier>(topicName, "SUP", sp2)).get();
				System.out.println(i);
			}
		} finally {
			System.out.println("SupplierProducer Completed.");

			producer.close();

		}
	}
}