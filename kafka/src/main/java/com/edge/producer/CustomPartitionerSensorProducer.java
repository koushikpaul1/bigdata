package com.edge.producer;

import java.util.*;
import org.apache.kafka.clients.producer.*;
import org.apache.kafka.common.Cluster;
import org.apache.kafka.common.PartitionInfo;
import org.apache.kafka.common.record.InvalidRecordException;
import org.apache.kafka.common.utils.Utils;
public class CustomPartitionerSensorProducer {

   public static void main(String[] args) throws Exception{

      String topicName = "SensorTopic";

      Properties props = new Properties();
      props.put("bootstrap.servers", "localhost:9092,localhost:9093");
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

class SensorPartitioner implements Partitioner {

	private String speedSensorName;

	public void configure(Map<String, ?> configs) {
		speedSensorName = configs.get("speed.sensor.name").toString();

	}

	public int partition(String topic, Object key, byte[] keyBytes, Object value, byte[] valueBytes, Cluster cluster) {

		List<PartitionInfo> partitions = cluster.partitionsForTopic(topic);
		int numPartitions = partitions.size();
		int sp = (int) Math.abs(numPartitions * 0.3);
		int p = 0;

		if ((keyBytes == null) || (!(key instanceof String)))
			throw new InvalidRecordException("All messages must have sensor name as key");

		if (((String) key).equals(speedSensorName))
			p = Utils.toPositive(Utils.murmur2(valueBytes)) % sp;
		else
			p = Utils.toPositive(Utils.murmur2(keyBytes)) % (numPartitions - sp) + sp;

		System.out.println("Key = " + (String) key + " Partition = " + p);
		return p;
	}

	public void close() {
	}

}