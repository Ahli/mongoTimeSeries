package mongodb.timeseries.demo.persistence.model;

import org.springframework.data.mongodb.core.mapping.Field;

public record MetaData(

		@Field("deviceId")
		String deviceId,

		@Field("dataType")
		String dataType

) { }
