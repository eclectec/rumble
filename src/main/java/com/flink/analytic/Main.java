package com.flink.analytic;

import org.apache.flink.api.common.serialization.SimpleStringSchema;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.table.api.DataTypes;
import org.apache.flink.table.api.Schema;
import org.apache.flink.table.api.EnvironmentSettings;
import org.apache.flink.table.api.TableDescriptor;
import org.apache.flink.table.api.TableEnvironment;
import org.apache.flink.connector.kafka.sink.KafkaRecordSerializationSchema;
import org.apache.flink.connector.kafka.sink.KafkaSink;
import org.apache.flink.connector.kafka.source.KafkaSource;
import org.apache.flink.connector.kafka.source.enumerator.initializer.OffsetsInitializer;
import java.util.concurrent.TimeUnit;

import org.apache.flink.api.common.eventtime.WatermarkStrategy;
import com.google.gson.*;


public class Main {

    public static Gson gson = new Gson();

    public static void main(String[] args) throws Exception {
        TimeUnit.SECONDS.sleep(20); // Wait for Docker to be up and running.

        final StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        final String bootstrapServers = "kafka:9093";

        KafkaSource<String> source = KafkaSource.<String>builder()
            .setBootstrapServers(bootstrapServers)
            .setTopics("traffic")
            .setGroupId("my-group")
            .setStartingOffsets(OffsetsInitializer.earliest())
            .setValueOnlyDeserializer(new SimpleStringSchema())
            .build();

        KafkaRecordSerializationSchema<String> serializer = KafkaRecordSerializationSchema.builder()
			.setValueSerializationSchema(new SimpleStringSchema())
			.setTopic("rumble")
			.build();

		KafkaSink<String> sink = KafkaSink.<String>builder()
			.setBootstrapServers(bootstrapServers)
			.setRecordSerializer(serializer)
			.build();

        DataStream<String> kafMessages = env.fromSource(source, WatermarkStrategy.noWatermarks(), "Traffic Source");

        DataStream<String> plots = kafMessages.map(message -> {
            JsonObject aircraft = gson.fromJson(message, JsonObject.class);
            JsonObject geoJsonObj = new JsonObject();
            geoJsonObj.addProperty("type", "Feature");
                        
            JsonObject geoObj = new JsonObject();
            geoObj.addProperty("type", "Point");
            
            JsonArray coords = new JsonArray();
            coords.add(aircraft.get("lon"));
            coords.add(aircraft.get("lat"));
            geoObj.add("coordinates", coords);	
            geoJsonObj.add("geometry", geoObj);
            
            JsonObject propertiesObject = new JsonObject();
            propertiesObject.add("icao", aircraft.get("hex"));
            propertiesObject.add("category", aircraft.get("category"));
            propertiesObject.add("speed", aircraft.get("gs"));

            String uniqueId = String.format("rumble-%s",aircraft.get("hex").toString().replaceAll("\"", "").trim());
            propertiesObject.addProperty("uid", uniqueId);

            geoJsonObj.add("properties", propertiesObject);

            return gson.toJson(geoJsonObj);
        });

        plots.sinkTo(sink);
        env.execute("Create Rumble Points");
    }
    
}
