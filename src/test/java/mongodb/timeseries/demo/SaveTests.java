package mongodb.timeseries.demo;

import mongodb.timeseries.demo.persistence.model.Measurement;
import mongodb.timeseries.demo.persistence.model.MetaData;
import mongodb.timeseries.demo.persistence.repository.MeasurementRepository;
import org.junit.jupiter.api.Test;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.boot.test.context.SpringBootTest;
import org.springframework.test.context.DynamicPropertyRegistry;
import org.springframework.test.context.DynamicPropertySource;
import org.testcontainers.containers.MongoDBContainer;
import org.testcontainers.junit.jupiter.Container;
import org.testcontainers.junit.jupiter.Testcontainers;
import org.testcontainers.utility.DockerImageName;

import java.time.Instant;
import java.time.temporal.ChronoUnit;
import java.util.ArrayList;
import java.util.List;

import static org.assertj.core.api.Assertions.assertThat;

@Testcontainers
@SpringBootTest
class SaveTests {
	
	@Container
	private static final MongoDBContainer mongo =
			new MongoDBContainer(DockerImageName.parse("mongo:6.0.2"));
	
	@Autowired
	private MeasurementRepository measurementRepository;
	
	@DynamicPropertySource
	static void modifyContext(DynamicPropertyRegistry registry) {
		registry.add("spring.data.mongodb.uri", mongo::getReplicaSetUrl);
	}
	
	@Test
	void testContainerWorks() {
		assertThat(mongo.isRunning()).isTrue();
	}
	
	@Test
	void testSave() {
		assertThat(measurementRepository.findAll()).isEmpty();
		
		MetaData metaData = new MetaData("device_1", "hoursOnline");

		// create 1000 measurements from 1 to 1000
		List<Measurement> measurements = new ArrayList<>(1000);
		Instant timestamp = Instant.parse("2022-11-03T18:00:00.00Z");
		float value = 1000;

		for (int i = 1000; i > 0; i--) {
			measurements.add(new Measurement(timestamp, metaData, value));
			timestamp = timestamp.minus(1, ChronoUnit.HOURS);
			value--;
		}
		
		measurementRepository.saveAll(measurements);
		
		assertThat(measurementRepository.findAll()).hasSize(1000);
	}
	
}
