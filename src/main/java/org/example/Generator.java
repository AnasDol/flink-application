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
import java.text.ParseException;
import java.text.SimpleDateFormat;
import java.util.*;

class CsvData implements Serializable {

	private Timestamp start_time;
	private String measuringProbeName;
	private String imsi;
	private String msisdn;
	private String msIpAddress;
	private long uniqueCdrId;



	public CsvData(Timestamp start_time, String measuringProbeName, String imsi, String msisdn, String msIpAddress, long uniqueCdrId) {
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

		SimpleDateFormat dateFormatter = new SimpleDateFormat("yyyy-MM-dd");

		static Random random = new Random();

		private long index = 0;

		private static final List<String> probes = Arrays.asList("DE", "cl", "ek", "ir", "kg", "kh", "mn", "nn", "ns", "rd", "sp", "sr", "st", "vr", "yd");

		@Override
		public void open(String s, FunctionInitializationContext functionInitializationContext, RuntimeContext runtimeContext) throws Exception {
			generator = new RandomDataGenerator();
			connection = DriverManager.getConnection(url, user, password);
//			preparedStatement = connection.prepareStatement("SELECT imsi, msisdn, ms_ip_address FROM public.ms_ip ORDER BY RANDOM()",
//					ResultSet.TYPE_SCROLL_INSENSITIVE, ResultSet.CONCUR_READ_ONLY);

			preparedStatement = connection.prepareStatement(
					"SELECT\n" +
							"    ms_ip.imsi AS imsi,\n" +
							"    ms_ip.msisdn AS msisdn,\n" +
							"    TRIM(ip_table) AS ip\n" +
							"FROM\n" +
							"    public.ms_ip,\n" +
							"    unnest(string_to_array(TRIM(ms_ip_address), ';')) AS ip_table\n" +
							"WHERE\n" +
							"    TRIM(ip_table) <> ''\n" +
							"ORDER BY\n" +
							"    RANDOM();",
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
						generateStartTime(
								dateFormatter.parse("2020-02-05").getTime(),
								dateFormatter.parse("2024-02-05").getTime()
						),
						generateMeasuringProbeName(probes),
						getValueWithProbability(resultSet.getLong("imsi"), 0.5),
						getValueWithProbability(resultSet.getLong("msisdn"), 0.1),
						getMsIpAddress(resultSet), // пока только один ip
						index++
				);

			} catch (SQLException | ParseException e) {
				throw new RuntimeException(e);
			}
		}

		public static Timestamp generateStartTime(long begin, long end) {
			return new Timestamp(begin + Math.abs(random.nextLong()) % (end - begin));
		}

		public static String generateMeasuringProbeName(List<String> probes) {
			return probes.get(random.nextInt(probes.size())) + "...";
		}

		public static String getValueWithProbability(long value, double p) {
			if (Math.random() < p) {
				return Long.toString(value);
			} else {
				return "";
			}
		}

		public static String getMsIpAddress(ResultSet resultSet) throws SQLException {
				return ";" + resultSet.getString("ip") + ";";
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
