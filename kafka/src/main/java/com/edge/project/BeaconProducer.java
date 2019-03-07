package com.edge.project;

import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.ResultSet;
import java.sql.ResultSetMetaData;
import java.sql.SQLException;
import java.sql.Statement;
import java.util.Properties;
import org.apache.commons.lang.StringUtils;
import org.apache.kafka.clients.producer.KafkaProducer;
import org.apache.kafka.clients.producer.Producer;
import org.apache.kafka.clients.producer.ProducerRecord;

public class BeaconProducer {

	ResultSet resultSet;
	int columns = 0;

	void getDataFromDB() throws ClassNotFoundException, SQLException, InstantiationException, IllegalAccessException {
		Class.forName("com.mysql.jdbc.Driver").newInstance();
		Connection connection = DriverManager
				.getConnection("jdbc:mysql://localhost:3306/autoscope?autoReconnect=true&useSSL=false", "root", "root");

		Statement statement = connection.createStatement();
		String query = "SELECT * FROM beacon_readings";
		resultSet = statement.executeQuery(query);
		columns = resultSet.getMetaData().getColumnCount();
	}

	void produce() throws SQLException, InterruptedException {
		Properties props = new Properties();
		props.put("bootstrap.servers", "192.168.85.133:9092");
		// props.put("bootstrap.servers",
		// "192.168.85.133:9092,192.168.85.133:9093,192.168.85.133:9094");
		props.put("key.serializer", "org.apache.kafka.common.serialization.StringSerializer");
		props.put("value.serializer", "org.apache.kafka.common.serialization.StringSerializer");
		String topic = "locator";
		Producer<String, String> producer = new KafkaProducer<>(props);
		ProducerRecord<String, String> record;
		int count = 0;
		String row;
		while (resultSet.next()) {
			row = "";
			for (int i = 1; i <= columns; i++) {
				row+= resultSet.getString(i) + ",";
				}
			record = new ProducerRecord<>(topic, Integer.toString(++count), row);
			System.out.println(Integer.toString(++count) + " => " + row);
			producer.send(record);
			Thread.sleep(1000);
		}
		producer.close();
	}

	public static void main(String[] a) throws ClassNotFoundException, SQLException, InstantiationException,
			IllegalAccessException, InterruptedException {
		BeaconProducer beaconProducer = new BeaconProducer();
		beaconProducer.getDataFromDB();
		beaconProducer.produce();
	}
}
