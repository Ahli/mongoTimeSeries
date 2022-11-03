package mongodb.timeseries.demo.persistence.repository;

import mongodb.timeseries.demo.persistence.dto.BucketDataDto;
import mongodb.timeseries.demo.persistence.model.Measurement;
import mongodb.timeseries.demo.persistence.model.MetaData;
import org.springframework.data.mongodb.repository.Aggregation;
import org.springframework.data.mongodb.repository.MongoRepository;
import org.springframework.data.mongodb.repository.Query;
import org.springframework.stereotype.Repository;

import java.time.Instant;
import java.util.List;

@Repository
public interface MeasurementRepository extends MongoRepository<Measurement, String> {
























    @Query("""
            {  'metaData.deviceId': :#{#metaData.deviceId()}
               'metaData.dataType': :#{#metaData.dataType()},
                timestamp:          { $gte: ?1, $lt: ?2 }
            }""")
    List<Measurement> findInInterval(MetaData metaData, Instant timeGE, Instant timeLT);


















    @Aggregation({
            "{ $match: { 'metaData.deviceId': :#{#metaData.deviceId()}," +
                    "    'metaData.dataType': :#{#metaData.dataType()}   } }",
            "{ $sort: { timestamp: -1 } }",
            "{ $limit: 1 }"})
    Measurement findLast(MetaData metaData);























    @Aggregation({
            "{ $match: { 'metaData.deviceId': :#{#metaData.deviceId()}," +
                    "    'metaData.dataType': :#{#metaData.dataType()}," +
                    "     timestamp:          { $gte: ?1, $lt: ?2 }      } } }",
            "{ $bucket: { groupBy: '$timestamp', boundaries: ?3, output: {" +
                    "     average: { $avg:  '$value' }," +
                    "     last:    { $last: '$value' } " +
                    " } } }",
            "{ $project: { startDate: '$_id', average: 1, last: 1, _id: 0 } }",
            "{ $sort: { startDate : 1 } }"
    })
    List<BucketDataDto> findBuckets(MetaData metaData, Instant timeGE, Instant timeLT, List<Instant> bucketBoundaries);

}
