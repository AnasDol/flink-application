package org.example;

import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.table.api.EnvironmentSettings;
import org.apache.flink.table.api.Table;
import org.apache.flink.table.api.bridge.java.StreamTableEnvironment;

import static org.apache.flink.table.api.Expressions.*;

public class KafkaConsoleReader {

    public static void main(String[] args) throws Exception {

        EnvironmentSettings settings = EnvironmentSettings
                .newInstance()
                .inStreamingMode()
                .build();

        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        StreamTableEnvironment tableEnv = StreamTableEnvironment.create(env);

        tableEnv.executeSql("CREATE TABLE example_table_kafka (" +
                "`id` DECIMAL," +
                "`number` BIGINT," +
                "`ip` STRING" +
                ") WITH (" +
                "'connector' = 'kafka'," +
                "'topic' = 'test2'," +
                "'value.format' = 'csv'," +
                "'value.csv.null-literal' = ''," +
                "'value.csv.ignore-parse-errors' = 'true'," +
                "'properties.bootstrap.servers' = 'kafka:9092'," +
                "'properties.group.id' = 'flink-table-group'," +
                "'scan.startup.mode' = 'latest-offset')");

        tableEnv.executeSql("CREATE TABLE example_print_table (" +
                "id DECIMAL(10,2)," +
                "number BIGINT," +
                "ip STRING" +
                ") WITH (" +
                "'connector' = 'print')");

        Table tableExample = tableEnv.from("example_table_kafka");
        tableExample
                .addOrReplaceColumns($("ip").replace(lit(" "), lit("")).as("ip"))
                .executeInsert("example_print_table");


    }
}
