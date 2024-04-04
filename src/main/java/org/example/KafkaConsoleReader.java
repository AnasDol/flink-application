package org.example;

import org.apache.flink.api.common.functions.AggregateFunction;
import org.apache.flink.api.common.typeinfo.Types;
import org.apache.flink.api.java.functions.KeySelector;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.datastream.DataStreamSink;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.windowing.assigners.ProcessingTimeSessionWindows;
import org.apache.flink.streaming.api.windowing.time.Time;
import org.apache.flink.table.api.EnvironmentSettings;
import org.apache.flink.table.api.Schema;
import org.apache.flink.table.api.Table;
import org.apache.flink.table.api.bridge.java.StreamTableEnvironment;
import org.apache.flink.table.functions.TableFunction;
import org.apache.flink.types.Row;

import java.time.LocalDateTime;

import static org.apache.flink.table.api.Expressions.*;

public class KafkaConsoleReader {

    public static void main(String[] args) throws Exception {

        EnvironmentSettings settings = EnvironmentSettings
                .newInstance()
                .inStreamingMode()
                .build();

        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();

        StreamTableEnvironment tableEnv = StreamTableEnvironment.create(env);
        tableEnv.registerFunction("split", new Split(";"));

        String jdbc_url = "jdbc:postgresql://host.docker.internal:5432/diploma";
        String jdbc_user = "postgres";
        String jdbc_password = "7844";

        tableEnv.executeSql(
                "CREATE TABLE src " +
                "(" +
                    "start_time TIMESTAMP," +
                    "measuring_probe_name STRING," +
                    "imsi BIGINT," +
                    "msisdn BIGINT," +
                    "ms_ip_address STRING," +
                    "unique_cdr_id BIGINT" +
                ") WITH (" +
                    "'connector' = 'kafka'," +
                    "'topic' = 'test3'," +
                    "'value.format' = 'csv'," +
                    "'value.csv.null-literal' = ''," +
                    "'value.csv.ignore-parse-errors' = 'true'," +
                    "'properties.bootstrap.servers' = 'kafka:9092'," +
                    "'properties.group.id' = 'flink-table-group'," +
                    "'scan.startup.mode' = 'latest-offset'" +
                ")");

        tableEnv.executeSql(
                "CREATE TEMPORARY VIEW src_extended AS " +
                        "SELECT *, " +
                        "  CAST(start_time AS DATE) AS event_date, " +
                        "  SUBSTRING(measuring_probe_name, 1, 2) AS probe " +
                        "FROM src"
        );



        tableEnv.executeSql(
                "CREATE TEMPORARY VIEW src_exploded AS " +
                        "SELECT src_extended.*, " +
                        "TRIM(ip) AS ip " +
                        "FROM src_extended, LATERAL TABLE(split(TRIM(ms_ip_address))) AS T(ip) " +
                        "WHERE TRIM(ip) <> ''"
        );

        tableEnv.executeSql("CREATE TABLE imsi_msisdn (" +
                "imsi BIGINT," +
                "msisdn BIGINT" +
                ") WITH (" +
                "'connector' = 'jdbc'," +
                "'url' = '" + jdbc_url + "'," +
                "'table-name' = '" + "public.imsi_msisdn" + "'," +
                "'username' = '" + jdbc_user + "'," +
                "'password' = '" + jdbc_password + "'" +
                ")");

        tableEnv.executeSql("CREATE TABLE ms_ip (" +
                "start_time TIMESTAMP," +
                "imsi BIGINT," +
                "msisdn BIGINT," +
                "ms_ip_address STRING " +
//                "probe AS _probe,"
                ") WITH (" +
                "'connector' = 'jdbc'," +
                "'url' = '" + jdbc_url + "'," +
                "'table-name' = '" + "public.ms_ip" + "'," +
                "'username' = '" + jdbc_user + "'," +
                "'password' = '" + jdbc_password + "'" +
                ")");

        tableEnv.executeSql(
                "CREATE TEMPORARY VIEW ms_ip_exploded AS " +
                        "SELECT " +
                        "ms_ip.imsi AS _imsi," +
                        "ms_ip.msisdn AS _msisdn," +
                        "ms_ip.start_time AS _start_time," +
//                        "ms_ip.probe AS _probe," +
                        "TRIM(_ip) AS _ip " +
                        "FROM ms_ip, LATERAL TABLE(split(TRIM(ms_ip_address))) AS T(_ip) " +
                        "WHERE TRIM(_ip) <> ''"
        );

        tableEnv.executeSql(
                "CREATE TEMPORARY VIEW joined_imsi_msisdn AS " +
                        "SELECT * " +
                        "FROM src_extended " +
                        "JOIN (" +
                        "   SELECT " +
                        "   imsi AS _imsi, " +
                        "   msisdn AS _msisdn" +
                        "   FROM imsi_msisdn" +
                        ") AS imsi_msisdn_renamed " +
                        "ON (" +
                        "imsi = _imsi" +
                        ") " +
                        "WHERE imsi IS NOT NULL "
//                        + "AND IMSI NOT LIKE '999%"

        );

        Table joinedImsiMsisdnTable = tableEnv.from("joined_imsi_msisdn")
                .addOrReplaceColumns(coalesce($("_imsi"), $("imsi")).as("imsi"))
                .addOrReplaceColumns(coalesce($("_msisdn"), $("msisdn")).as("msisdn"))
                .select(
                        $("start_time"), // херня, лучше было бы найти способ через схему. либо переделать на SQL
                        $("measuring_probe_name"),
                        $("imsi"),
                        $("msisdn"),
                        $("ms_ip_address"),
                        $("unique_cdr_id"),
                        $("event_date"),
                        $("probe")
                );

        System.out.println("joinedImsiMsisdnTable schema:");
        joinedImsiMsisdnTable.printSchema();

        tableEnv.executeSql(
                "CREATE TEMPORARY VIEW joined_msip AS " +
                        "SELECT * " +
                        "FROM src_exploded " +
                        "JOIN ms_ip_exploded " +
                        "ON (" +
//                        "probe = _probe " +
//                        "AND " +
                        "ip = _ip " +
                        "AND " +
                        "start_time >= _start_time" +
                        ") " +
                        "WHERE imsi IS NULL "
//                        + "OR IMSI LIKE '999%"

        );



        Table joinedMsipTable = tableEnv.from("joined_msip")
                .addOrReplaceColumns(coalesce($("_imsi"), $("imsi")).as("imsi"))
                .addOrReplaceColumns(coalesce($("_msisdn"), $("msisdn")).as("msisdn"));

        Schema joinedMsipSchema = joinedMsipTable.getSchema().toSchema();



        // keyed session windows
        DataStream<Row> joined_msip_DS = tableEnv.toDataStream(joinedMsipTable);
        DataStream<Row> filtered = joined_msip_DS
                .keyBy((KeySelector<Row, Object>) value -> value.getField("unique_cdr_id"))
                .window(ProcessingTimeSessionWindows.withGap(Time.milliseconds(3000)))
                .aggregate(new MaxStartTimeAggregate());



//        tableEnv.registerDataStream(
//                "filtered",
//                filtered
//        );

        Table filteredTable = tableEnv
                .fromDataStream(filtered.map(x -> x).returns(Types.ROW(
                        Types.LOCAL_DATE_TIME,
                        Types.STRING,
                        Types.LONG,
                        Types.LONG,
                        Types.STRING,
                        Types.LONG,
                        Types.LOCAL_DATE,
                        Types.STRING,
                        Types.STRING,
                        Types.LONG,
                        Types.LONG,
                        Types.LOCAL_DATE_TIME,
                        Types.STRING)))
                .as(
                        "start_time",
                        "measuring_probe_name",
                        "imsi",
                        "msisdn",
                        "ms_ip_address",
                        "unique_cdr_id",
                        "event_date",
                        "probe",
                        "ip",
                        "_imsi",
                        "_msisdn",
                        "_start_time",
                        "_ip"
                )
                .select(
                        $("start_time"), // херня, лучше было бы найти способ через схему. либо переделать на SQL
                        $("measuring_probe_name"),
                        $("imsi"),
                        $("msisdn"),
                        $("ms_ip_address"),
                        $("unique_cdr_id"),
                        $("event_date"),
                        $("probe")
                );

        System.out.println("filteredTable schema:");
        filteredTable.printSchema();


        tableEnv.executeSql(
                "CREATE TABLE sinkTable " +
                        "(" +
                        "start_time TIMESTAMP," +
                        "measuring_probe_name STRING," +
                        "imsi BIGINT," +
                        "msisdn BIGINT," +
                        "ms_ip_address STRING," +
                        "unique_cdr_id BIGINT," +
                        "event_date DATE," +
                        "probe STRING" +
                        ") PARTITIONED BY (event_date, probe) WITH (" +
                        "'connector' = 'filesystem'," +
                        "'path' = './flink_parquet_results'," +
                        "'format' = 'parquet'," +
                        "'sink.rolling-policy.file-size' = '110MB'," +
                        "'sink.rolling-policy.check-interval' = '5 s'," +
                        "'sink.rolling-policy.rollover-interval' = '20 s'" +
                        ")");



        // sink в консоль

        DataStreamSink<Row> printSink1 = tableEnv
                .toAppendStream(filteredTable, Row.class)
                .print("printSink1 (filteredTable)");

        DataStreamSink<Row> printSink2 = tableEnv
                .toAppendStream(joinedImsiMsisdnTable, Row.class)
                .print("printSink2 (joinedImsiMsisdnTable)");

        // sink в фс

        filteredTable.insertInto("sinkTable").execute();
        joinedImsiMsisdnTable.insertInto("sinkTable").execute();


        env.execute("Kafka Console Reader");

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

    public static class MaxStartTimeAggregate implements AggregateFunction<Row, Row, Row> {

        @Override
        public Row createAccumulator() {
            return null;
        }

        @Override
        public Row add(Row value, Row accumulator) {
            if (accumulator == null || ((LocalDateTime) value.getField("_start_time")).isAfter((LocalDateTime) accumulator.getField("_start_time"))) {
                return value;
            }
            return accumulator;
        }

        @Override
        public Row getResult(Row accumulator) {
            return accumulator;
        }

        @Override
        public Row merge(Row a, Row b) {
            if (a == null || ((LocalDateTime) b.getField("_start_time")).isAfter((LocalDateTime) a.getField("_start_time"))) {
                return b; // Merge two accumulators by keeping the one with larger _start_time
            }
            return a;
        }
    }
}
