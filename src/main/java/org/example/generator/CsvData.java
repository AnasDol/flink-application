package org.example.generator;

import com.typesafe.config.Config;
import org.apache.flink.api.common.functions.RuntimeContext;
import org.apache.flink.runtime.state.FunctionInitializationContext;
import org.apache.flink.streaming.api.functions.source.datagen.DataGenerator;

import java.io.Serializable;
import java.sql.*;
import java.util.Arrays;
import java.util.List;
import java.util.Random;

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

    @Override
    public String toString() {
        return start_time +
                "," + measuringProbeName +
                "," + imsi +
                "," + msisdn +
                "," + msIpAddress +
                "," + uniqueCdrId;
    }

    static class CsvDataGenerator implements DataGenerator<CsvData> {

        private final Config config;
        private Connection connection;
        private PreparedStatement preparedStatement;
        private ResultSet resultSet;
        static Random random = new Random();
        private final List<String> probes = Arrays.asList("DE", "cl", "ek", "ir", "kg", "kh", "mn", "nn", "ns", "rd", "sp", "sr", "st", "vr", "yd");

        public CsvDataGenerator(Config config) {
            this.config = config;
        }

        @Override
        public void open(String s, FunctionInitializationContext functionInitializationContext, RuntimeContext runtimeContext) throws Exception {

            connection = DriverManager.getConnection(
                    config.getString("ms_ip.url"),
                    config.getString("ms_ip.user"),
                    config.getString("ms_ip.password"));

            String sqlStatement =
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
                    "    RANDOM();";

            preparedStatement = connection.prepareStatement(sqlStatement,
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
                    resultSet.first();
                }

                return new CsvData(
                        generateStartTime(
                                config.getLong("generator.startTime_min"),
                                config.getLong("generator.startTime_max")
                        ),
                        generateMeasuringProbeName(probes),
                        getValueWithProbability(
                                resultSet.getLong("imsi"),
                                config.getDouble("generator.imsiNotNullProbability")
                        ), // с вероятностью p получаем не пустое значение
                        getValueWithProbability(
                                resultSet.getLong("msisdn"),
                                config.getDouble("generator.msisdnNotNullProbability")
                        ),
                        getMsIpAddress(resultSet), // пока только один ip
                        generateUniqueCdrId()
                );

            } catch (SQLException e) {
                throw new RuntimeException(e);
            }
        }

        @Override
        protected void finalize() throws Throwable {
            super.finalize();
            resultSet.close();
            preparedStatement.close();
            connection.close();
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

        public static long generateUniqueCdrId() {
            String cdrID = System.currentTimeMillis() + "" + random.nextInt(1000000);
            return Long.parseLong(cdrID);
        }

    }

}
