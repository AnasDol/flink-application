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

        Table resultTable = tableEnv.from("src_exploded");
        DataStreamSink<Row> dataStreamSink = tableEnv
                .toAppendStream(resultTable, Row.class)
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
