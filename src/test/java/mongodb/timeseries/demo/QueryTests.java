package mongodb.timeseries.demo;

import mongodb.timeseries.demo.persistence.dto.BucketDataDto;
import mongodb.timeseries.demo.persistence.model.Measurement;
import mongodb.timeseries.demo.persistence.model.MetaData;
import mongodb.timeseries.demo.persistence.repository.MeasurementRepository;
import org.junit.jupiter.api.BeforeEach;
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
class QueryTests {

    @Container
    private static final MongoDBContainer mongo =
            new MongoDBContainer(DockerImageName.parse("mongo:6.0.2"));

    @Autowired
    private MeasurementRepository measurementRepository;

    @DynamicPropertySource
    static void modifyContext(DynamicPropertyRegistry registry) {
        registry.add("spring.data.mongodb.uri", mongo::getReplicaSetUrl);
    }

    @BeforeEach
    void saveData() {
        measurementRepository.deleteAll();
        MetaData metaData = new MetaData("device_1", "hoursOnline");
        Instant timestamp = Instant.parse("2022-11-03T18:00:00.00Z");
        List<Measurement> measurements = new ArrayList<>(1000);
        float value = 1000f;
        for (int i = 1000; i > 0; i--) {
            measurements.add(new Measurement(timestamp, metaData, value));
            timestamp = timestamp.minus(1, ChronoUnit.HOURS);
            value--;
        }
        measurementRepository.saveAll(measurements);
    }

    @Test
    void testFindInterval() {
        MetaData metaData = new MetaData("device_1", "hoursOnline");
        Instant startGE = Instant.parse("2022-11-02T00:00:00.00Z");
        Instant endLT = Instant.parse("2022-11-03T00:00:00.00Z");
        List<Measurement> foundInInterval =
                measurementRepository.findInInterval(metaData, startGE, endLT);

        assertThat(foundInInterval).hasSize(24);
    }

    @Test
    void testFindLast() {
        MetaData metaData = new MetaData("device_1", "hoursOnline");
        Measurement last = measurementRepository.findLast(metaData);

        assertThat(last.getValue()).isEqualTo(1000);
        assertThat(last.getTimestamp()).isEqualTo(Instant.parse("2022-11-03T18:00:00.00Z"));
    }

    @Test
    void testGetBucket() {
        MetaData metaData = new MetaData("device_1", "hoursOnline");
        Instant startGE = Instant.parse("2022-11-02T18:00:00.00Z");
        Instant mid     = Instant.parse("2022-11-03T06:00:00.00Z");
        Instant endLT   = Instant.parse("2022-11-03T18:00:00.00Z");
        List<Instant> boundaries = List.of(startGE, mid, endLT);
        List<BucketDataDto> buckets =
                measurementRepository.findBuckets(metaData, startGE, endLT, boundaries);

        assertThat(buckets).hasSize(2);

        assertThat(buckets.get(0).startDate()).isEqualTo(startGE);
        assertThat(buckets.get(0).average()).isEqualTo(981.5);
        assertThat(buckets.get(0).last()).isEqualTo(987);

        assertThat(buckets.get(1).startDate()).isEqualTo(mid);
        assertThat(buckets.get(1).average()).isEqualTo(993.5);
        assertThat(buckets.get(1).last()).isEqualTo(999);
    }

}
