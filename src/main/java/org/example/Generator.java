package org.example;

import com.typesafe.config.Config;
import com.typesafe.config.ConfigFactory;
import org.apache.commons.math3.random.RandomDataGenerator;
import org.apache.flink.api.common.functions.RuntimeContext;
import org.apache.flink.api.common.typeinfo.TypeHint;
import org.apache.flink.api.common.typeinfo.TypeInformation;
import org.apache.flink.runtime.state.FunctionInitializationContext;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.functions.source.datagen.DataGenerator;
import org.apache.flink.streaming.api.functions.source.datagen.DataGeneratorSource;
import org.apache.flink.table.api.EnvironmentSettings;
import org.apache.flink.table.api.bridge.java.StreamTableEnvironment;
import org.apache.flink.table.functions.TableFunction;


import java.io.Serializable;
import java.sql.*;

class CsvData implements Serializable {

	private Timestamp start_time;
	private String measuringProbeName;
	private long imsi;
	private long msisdn;
	private String msIpAddress;
	private long uniqueCdrId;

	private static int index = 0;

	public CsvData(Timestamp start_time, String measuringProbeName, long imsi, long msisdn, String msIpAddress, long uniqueCdrId) {
		this.start_time = start_time;
		this.measuringProbeName = measuringProbeName;
		this.imsi = imsi;
		this.msisdn = msisdn;
		this.msIpAddress = msIpAddress;
		this.uniqueCdrId = uniqueCdrId;
	}

	static class CsvDataGenerator implements DataGenerator<CsvData> {

		String url = "jdbc:postgresql://host.docker.internal:5432/diploma";
		String user = "postgres";
		String password = "7844";
		Connection connection;
		RandomDataGenerator generator;
		private PreparedStatement preparedStatement;
		ResultSet resultSet;

		@Override
		public void open(String s, FunctionInitializationContext functionInitializationContext, RuntimeContext runtimeContext) throws Exception {
			generator = new RandomDataGenerator();
			connection = DriverManager.getConnection(url, user, password);
			preparedStatement = connection.prepareStatement("SELECT imsi, msisdn, ms_ip_address FROM public.ms_ip ORDER BY RANDOM()",
					ResultSet.TYPE_SCROLL_INSENSITIVE, ResultSet.CONCUR_READ_ONLY);

			resultSet = preparedStatement.executeQuery();
		}

		@Override
		public boolean hasNext() {
			return true;
		}

		@Override
		public CsvData next() {

			try {

				if (!resultSet.next()) {
					resultSet.close();
					resultSet = preparedStatement.executeQuery();
				}

				return new CsvData(
						new Timestamp(2020, 5, 1, 12, 50, 30, 123),
						"DEFAULT",
						resultSet.getLong("imsi"),
						resultSet.getLong("msisdn"),
						";63.59.250.208;",
						index++
				);

			} catch (SQLException e) {
				throw new RuntimeException(e);
			}
		}
	}

	@Override
	public String toString() {
		return "CsvData{" +
				"start_time=" + start_time +
				", measuringProbeName='" + measuringProbeName + '\'' +
				", imsi=" + imsi +
				", msisdn=" + msisdn +
				", msIpAddress='" + msIpAddress + '\'' +
				", uniqueCdrId=" + uniqueCdrId +
				'}';
	}
}

public class Generator {

	private static Config config;
	private static EnvironmentSettings settings;
	private static StreamExecutionEnvironment env;
	private static StreamTableEnvironment tEnv;

	public static void main(String[] args) throws Exception {

		config = ConfigFactory.load("flink.conf");
		settings = EnvironmentSettings.newInstance().inStreamingMode().build();
		env = StreamExecutionEnvironment.getExecutionEnvironment();
		tEnv = StreamTableEnvironment.create(env);
		tEnv.registerFunction("split", new Split(";"));


		DataGeneratorSource<CsvData> dataGeneratorSource = new DataGeneratorSource<>(
				new CsvData.CsvDataGenerator(), 1L, null);

		env.addSource(dataGeneratorSource).returns(new TypeHint<CsvData>() {
			@Override
			public TypeInformation<CsvData> getTypeInfo() {
				return super.getTypeInfo();
			}
		}).print();


		env.execute("Generator");

	}


	public static class Split extends TableFunction<String> {
		private String separator = ",";

		public Split(String separator){
			this.separator = separator;
		}

		public void eval(String str){
			for (String s: str.split(separator)){
				collect(s);
			}
		}
	}
}
