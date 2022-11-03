package mongodb.timeseries.demo.persistence.config;

import lombok.RequiredArgsConstructor;
import mongodb.timeseries.demo.persistence.model.Measurement;
import org.bson.Document;
import org.springframework.context.annotation.Configuration;
import org.springframework.data.mongodb.core.MongoTemplate;
import org.springframework.data.mongodb.core.index.CompoundIndexDefinition;

import javax.annotation.PostConstruct;

@RequiredArgsConstructor
@Configuration
public class PersistenceConfig {

    private final MongoTemplate mongoTemplate;

    @PostConstruct
    public void init() {
        ensureCollectionExists(Measurement.class);

        ensureMetaDataTimestampIndex(Measurement.class);
    }

    private void ensureCollectionExists(Class<?> collectionClass) {
        if (!mongoTemplate.collectionExists(collectionClass)) {
            mongoTemplate.createCollection(collectionClass);
        }
    }

    private void ensureMetaDataTimestampIndex(Class<?> collectionClass) {
        mongoTemplate.indexOps(collectionClass).ensureIndex(
                new CompoundIndexDefinition(new Document()
                        .append("metaData.deviceId", 1)
                        .append("metaData.dataType", 1)
                        .append("timestamp", 1)));
    }

}
