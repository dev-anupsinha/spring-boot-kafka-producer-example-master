package com.techprimers.kafka.springbootkafkaproducerexample.resource;

import com.techprimers.kafka.springbootkafkaproducerexample.model.User;

import java.io.File;
import java.io.FileInputStream;
import java.io.FileOutputStream;
import java.nio.file.Files;
import java.nio.file.Paths;

import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.kafka.core.KafkaTemplate;
import org.springframework.web.bind.annotation.GetMapping;
import org.springframework.web.bind.annotation.PathVariable;
import org.springframework.web.bind.annotation.RequestMapping;
import org.springframework.web.bind.annotation.RestController;

@RestController
@RequestMapping("kafka")
public class UserResource {

	@Autowired
	private KafkaTemplate<String, User> kafkaTemplate;

	private static final String TOPIC = "Kafka_Example";

	final int BUFFERSIZE = 1000000;//40 * 1024;
	String sourceFilePath = "C:\\Users\\AnupS\\Downloads\\NYSOH_FunctionalKT_5.mp4";

	@GetMapping("/publish/{name}")
	public String post(@PathVariable("name") final String name) {
		// kafkaTemplate.send(TOPIC, new User(name, "TechnologyAB", 2308));

		try (FileInputStream fin = new FileInputStream(new File(sourceFilePath));

		) {

			byte[] buffer = new byte[BUFFERSIZE];
			int bytesRead;
			int i=0;
			while (fin.available() != 0) {
				bytesRead = fin.read(buffer);
				System.out.println("buffer send" + bytesRead +"i value :"+ i);
				kafkaTemplate.send(TOPIC, new User(name, "Technology-"+ i++ , bytesRead));
			}
			

		} catch (Exception e) {
			System.out.println("Something went wrong! Reason: " + e.getMessage());
		}
		kafkaTemplate.send(TOPIC, new User(name, "Technology", 0));
		return "Published successfully";
	}
}
