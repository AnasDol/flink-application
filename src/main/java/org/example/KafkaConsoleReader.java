package org.example;

import org.apache.flink.streaming.api.datastream.DataStreamSink;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.table.api.EnvironmentSettings;
import org.apache.flink.table.api.Expressions;
import org.apache.flink.table.api.Table;
import org.apache.flink.table.api.bridge.java.StreamTableEnvironment;
import org.apache.flink.types.Row;

import static org.apache.flink.table.api.Expressions.*;

public class KafkaConsoleReader {

    public static void main(String[] args) throws Exception {

        EnvironmentSettings settings = EnvironmentSettings
                .newInstance()
                .inStreamingMode()
                .build();

        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        StreamTableEnvironment tableEnv = StreamTableEnvironment.create(env);

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

//        Table src = tableEnv.from("src");
//
//        Table src_extended = src
//                .select($("*"))
//                .addColumns(
//                        $("start_time").toDate().as("event_date"),
//                        $("measuring_probe_name").substring(1,2).as("probe")
//                );

        tableEnv.executeSql(
                "CREATE TEMPORARY VIEW src_extended AS " +
                        "SELECT *, " +
                        "  CAST(start_time AS DATE) AS event_date, " +
                        "  SUBSTRING(measuring_probe_name, 1, 2) AS probe " +
                        "FROM src"
        );

        Table resultTable = tableEnv.from("src_extended");
        DataStreamSink<Row> dataStreamSink = tableEnv
                .toAppendStream(resultTable, Row.class)
                .print();

        // Запуск приложения
        env.execute("Kafka Console Reader");




//        tableEnv.executeSql("CREATE TABLE example_table_kafka (" +
//                "`id` DECIMAL," +
//                "`number` BIGINT," +
//                "`ip` STRING" +
//                ") WITH (" +
//                "'connector' = 'kafka'," +
//                "'topic' = 'test2'," +
//                "'value.format' = 'csv'," +
//                "'value.csv.null-literal' = ''," +
//                "'value.csv.ignore-parse-errors' = 'true'," +
//                "'properties.bootstrap.servers' = 'kafka:9092'," +
//                "'properties.group.id' = 'flink-table-group'," +
//                "'scan.startup.mode' = 'latest-offset')");
//
//        tableEnv.executeSql("CREATE TABLE example_print_table (" +
//                "id DECIMAL(10,2)," +
//                "number BIGINT," +
//                "ip STRING" +
//                ") WITH (" +
//                "'connector' = 'filesystem'," +
//                "'path' = 'hdfs://namenode:9000/'," +
//                "'format' = 'csv'," +
//                "'partition.default-name' = 'my-file_')");
//
//        Table tableExample = tableEnv.from("example_table_kafka");
//        tableExample
//                .addOrReplaceColumns($("ip").replace(lit(" "), lit("")).as("ip"))
//                .executeInsert("example_print_table");


    }
}
