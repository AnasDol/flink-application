package org.example;

import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.streaming.api.datastream.DataStreamSink;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.table.api.*;
import org.apache.flink.table.api.bridge.java.StreamTableEnvironment;
import org.apache.flink.table.catalog.ResolvedSchema;
import org.apache.flink.table.functions.TableFunction;
import org.apache.flink.types.Row;

import java.util.ArrayList;
import java.util.List;

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
                    "`start_time` TIMESTAMP," +
                    "`measuring_probe_name` STRING," +
                    "`imsi` BIGINT," +
                    "`msisdn` BIGINT," +
                    "`ms_ip_address` STRING" +
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

        ResolvedSchema schema = tableEnv.from("src_extended").getResolvedSchema();

        tableEnv.executeSql(
                "CREATE TEMPORARY VIEW src_exploded AS " +
                        "SELECT src_extended.*, " +
                        "TRIM(ip) AS ip " +
                        "FROM src_extended, LATERAL TABLE(split(TRIM(ms_ip_address))) AS T(ip) " +
                        "WHERE TRIM(ip) <> ''"
        );

//        ResolvedSchema schema2 = tableEnv.from("src_exploded").getResolvedSchema();
//        System.out.println(schema2.toString());

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


        Table resultTable = tableEnv.from("joined_msip");
        DataStreamSink<Row> dataStreamSink = tableEnv
                .toAppendStream(resultTable, Row.class)
                .print();

        Table resultTable2 = tableEnv.from("joined_imsi_msisdn");
        DataStreamSink<Row> dataStreamSink2 = tableEnv
                .toAppendStream(resultTable2, Row.class)
                .print();

        // Запуск приложения
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
}
