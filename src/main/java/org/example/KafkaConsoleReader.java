package org.example;

import org.apache.flink.api.common.functions.AggregateFunction;
import org.apache.flink.api.java.functions.KeySelector;
import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.datastream.DataStreamSink;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.windowing.assigners.EventTimeSessionWindows;
import org.apache.flink.streaming.api.windowing.assigners.ProcessingTimeSessionWindows;
import org.apache.flink.streaming.api.windowing.time.Time;
import org.apache.flink.table.api.*;
import org.apache.flink.table.api.bridge.java.StreamTableEnvironment;
import org.apache.flink.table.catalog.ResolvedSchema;
import org.apache.flink.table.functions.TableFunction;
import org.apache.flink.types.Row;

import java.time.LocalDateTime;
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


//        // option 1 - use TopN statement. Problem - not append-only mode
//        tableEnv.executeSql(
//                "CREATE TEMPORARY VIEW max_start_time_row AS " +
//                        "SELECT * " +
//                        "FROM (" +
//                        "SELECT *, " +
//                        "ROW_NUMBER() OVER (PARTITION BY unique_cdr_id ORDER BY _start_time DESC) AS row_num " +
//                        "FROM joined_msip) " +
//                        "WHERE row_num = 1"
//        );


        // option 2 - use keyed session window

        DataStream<Row> joined_msip_DS = tableEnv.toDataStream(tableEnv.from("joined_msip"));

        // Применяем ключевое поле для группировки
        DataStream<Row> keyedStream = joined_msip_DS
                .keyBy((KeySelector<Row, Object>) value -> value.getField("unique_cdr_id"))
                .window(ProcessingTimeSessionWindows.withGap(Time.milliseconds(1000)))
                .aggregate(new MaxStartTimeAggregate());


        DataStreamSink<Row> dataStreamSink = keyedStream.print();

        Table resultTable = tableEnv.from("joined_imsi_msisdn");
        DataStreamSink<Row> dataStreamSink2 = tableEnv
                .toAppendStream(resultTable, Row.class)
                .print();



//        Table resultTable = tableEnv.from("joined_msip");
//        DataStreamSink<Row> dataStreamSink = tableEnv
//                .toAppendStream(resultTable, Row.class)
//                .print();
//


//        Table resultTable = tableEnv.from("max_start_time_row");
//        DataStreamSink<Tuple2<Boolean, Row>> dataStreamSink = tableEnv
//                .toRetractStream(resultTable, Row.class)
//                .print();

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
