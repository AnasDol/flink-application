package org.example.generator;

import com.typesafe.config.Config;
import com.typesafe.config.ConfigFactory;
import org.apache.flink.api.common.functions.MapFunction;
import org.apache.flink.api.common.serialization.SimpleStringSchema;
import org.apache.flink.api.common.typeinfo.TypeHint;
import org.apache.flink.connector.kafka.sink.KafkaRecordSerializationSchema;
import org.apache.flink.connector.kafka.sink.KafkaSink;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.functions.source.datagen.DataGeneratorSource;
import org.apache.flink.table.api.EnvironmentSettings;
import org.apache.flink.table.api.bridge.java.StreamTableEnvironment;

import java.io.File;

public class Generator {

	private static Config config;
	private static EnvironmentSettings settings;
	private static StreamExecutionEnvironment env;
	private static StreamTableEnvironment tEnv;

	public static void main(String[] args) throws Exception {

		if (args.length > 0) {
			String path = args[0];
			config = ConfigFactory.parseFile(new File(path));
		} else {
			config = ConfigFactory.load("flink.conf");
		}

		settings = EnvironmentSettings.newInstance().inStreamingMode().build();
		env = StreamExecutionEnvironment.getExecutionEnvironment();

		DataGeneratorSource<CsvData> dataGeneratorSource =
				new DataGeneratorSource<>(
						new CsvData.CsvDataGenerator(config),
						config.getInt("generator.rowsPerSecond"),
						config.getLong("generator.numberOfRows") > 0L ? config.getLong("generator.numberOfRows") : null
				);

		DataStream<CsvData> dataStream = env.addSource(dataGeneratorSource)
				.returns(new TypeHint<CsvData>() {})
				.name("CsvData Source");

		DataStream<String> stringDataStream = dataStream.map((MapFunction<CsvData, String>) CsvData::toString);

		KafkaSink<String> sink = KafkaSink.<String>builder()
				.setBootstrapServers(config.getString("kafka.bootstrap.servers"))
				.setRecordSerializer(KafkaRecordSerializationSchema.builder()
						.setTopic(config.getString("kafka.topic"))
						.setValueSerializationSchema(new SimpleStringSchema())
						.build()
				)
				.build();

		stringDataStream.sinkTo(sink);


		env.execute("flink-kafka-generator");

	}

}
