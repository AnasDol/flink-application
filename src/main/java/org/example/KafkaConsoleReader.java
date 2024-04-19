package org.example;

import com.typesafe.config.Config;
import com.typesafe.config.ConfigFactory;
import org.apache.flink.api.common.functions.AggregateFunction;
import org.apache.flink.api.common.typeinfo.TypeInformation;
import org.apache.flink.api.common.typeinfo.Types;
import org.apache.flink.api.java.functions.KeySelector;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.datastream.DataStreamSink;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.windowing.assigners.ProcessingTimeSessionWindows;
import org.apache.flink.streaming.api.windowing.time.Time;
import org.apache.flink.table.api.*;
import org.apache.flink.table.api.bridge.java.StreamTableEnvironment;
import org.apache.flink.table.expressions.Expression;
import org.apache.flink.table.functions.TableFunction;
import org.apache.flink.table.types.DataType;
import org.apache.flink.types.Row;

import java.io.File;
import java.time.LocalDateTime;
import java.util.Arrays;
import java.util.List;

import static org.apache.flink.table.api.Expressions.*;

public class KafkaConsoleReader {

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
        env.enableCheckpointing(config.getLong("checkpointing.interval"));

        tEnv = StreamTableEnvironment.create(env);
        tEnv.registerFunction("split", new Split(";"));

        run();

        env.execute("flink-enrichment-app");

    }

    public static void run() {

        createSrc();
        Table src_extended = extendSrcWithPartitionColumns(tEnv.from("src"));
        String[] outputFieldNames = src_extended.getSchema().getFieldNames(); // "src_extended" совпадает со схемой выходной строки
        explodeSrcExtended();


        createImsiMsisdn();
        joinImsiMsisdn();
        Table joinedImsiMsisdnTable = tEnv.from("joined_imsi_msisdn");
        joinedImsiMsisdnTable = updateImsiAndMsisdn(joinedImsiMsisdnTable);
        joinedImsiMsisdnTable = selectColumns(joinedImsiMsisdnTable, outputFieldNames);

        System.out.println("joinedImsiMsisdnTable schema:");
        joinedImsiMsisdnTable.printSchema();


        createMsIp();
        explodeMsIp();
        joinMsIp();
        Table joinedMsipTable = tEnv.from("joined_msip");
        joinedMsipTable = updateImsiAndMsisdn(joinedMsipTable);
        Table filteredTable = filterByMaxStartTime(joinedMsipTable);
        filteredTable = selectColumns(filteredTable, outputFieldNames);

        System.out.println("filteredTable schema:");
        filteredTable.printSchema();


        createSink();

        // sink в консоль

        DataStreamSink<Row> printSink1 = tEnv
                .toAppendStream(filteredTable, Row.class)
                .print("printSink1 (filteredTable)");

        DataStreamSink<Row> printSink2 = tEnv
                .toAppendStream(joinedImsiMsisdnTable, Row.class)
                .print("printSink2 (joinedImsiMsisdnTable)");

        // sink в фс - вызывает UnsupportedFileSystemSchemeException: Could not find a file system implementation for scheme 'hdfs'.

//        filteredTable.insertInto("sinkTable").execute();
//        joinedImsiMsisdnTable.insertInto("sinkTable").execute();

    }

    public static Table convertToTable(DataStream<Row> ds, TypeInformation<Row> typeInfo, String[] columnNames) {
        return tEnv
                .fromDataStream(ds.map(x -> x).returns(typeInfo))
                .as(columnNames[0], Arrays.copyOfRange(columnNames, 1, columnNames.length));
    }

    public static Table selectColumns(Table table, String[] columnNames) {
        Expression[] expressions = Arrays.stream(columnNames)
                .map(Expressions::$)
                .toArray(Expression[]::new);
        return table.select(expressions);
    }

    /**
     * В поля imsi и msisdn копируются соответствующие значения из полей _imsi и _msisdn
     */
    public static Table updateImsiAndMsisdn(Table table) {
        return table
                .addOrReplaceColumns(coalesce($("_imsi"), $("imsi")).as("imsi"))
                .addOrReplaceColumns(coalesce($("_msisdn"), $("msisdn")).as("msisdn"));
    }


    /**
     * Метод оставляет в таблице только строки с максимальным значением поля _start_time для каждого уникального unique_cdr_id.
     * Таблица преобразуется в датастрим, фильтруется с помощью сессионных окон и агрегатирующей функции MaxStartTimeAggregate
     * и снова преобразуется к таблице.
     */
    public static Table filterByMaxStartTime(Table table) {

        // keyed session windows
        DataStream<Row> filtered = tEnv.toDataStream(table)
                .keyBy((KeySelector<Row, Object>) value -> value.getField("unique_cdr_id"))
                .window(ProcessingTimeSessionWindows.withGap(Time.milliseconds(3000)))
                .aggregate(new MaxStartTimeAggregate());

        // получаем типы данных и имена колонок, чтобы восстановить таблицу из датастрима
        TypeInformation<?>[] columnTypes = table.getSchema().getFieldTypes();
        TypeInformation<Row> filteredTypeInfo = Types.ROW(columnTypes);
        String[] fieldNames = table.getSchema().getFieldNames();

        return convertToTable(filtered, filteredTypeInfo, fieldNames);
    }



    public static void createSrc() {
        String src =
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
                        "'topic' = '" + config.getString("kafka.topic") + "'," +
                        "'value.format' = '" + config.getString("kafka.format") + "'," +
                        "'value.csv.null-literal' = ''," +
                        "'value.csv.ignore-parse-errors' = 'true'," +
                        "'properties.bootstrap.servers' = '" + config.getString("kafka.bootstrap.servers") + "'," +
                        "'properties.group.id' = '" + config.getString("kafka.group_id") + "'," +
                        "'scan.startup.mode' = 'latest-offset'" +
                        ")";
        tEnv.executeSql(src);
    }

    public static void createSink() {
        String sink =
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
                        "'path' = '" + config.getString("hdfs.path") + "', "+
                        "'format' = '" + config.getString("hdfs.format") + "'," +
                        "'sink.rolling-policy.file-size' = '" + config.getString("hdfs.fileSize") + "'," +
                        "'sink.rolling-policy.check-interval' = '5 s'," +
                        "'sink.rolling-policy.rollover-interval' = '20 s'" +
                        ")";
        tEnv.executeSql(sink);
    }



    public static Table extendSrcWithPartitionColumns(Table src) {
        Table src_extended = src
                .select(
                        $("*"),
                        $("start_time").cast(DataTypes.DATE()).as("event_date"),
                        $("measuring_probe_name").substring(1,2).as("probe")
                );
        // Регистрируем расширенную таблицу во временном представлении
        tEnv.createTemporaryView("src_extended", src_extended);
        return src_extended;
    }

//    public static void extendSrcWithPartitionColumns() {
//        String srcExtended =
//                "CREATE TEMPORARY VIEW src_extended AS " +
//                        "SELECT *, " +
//                        "  CAST(start_time AS DATE) AS event_date, " +
//                        "  SUBSTRING(measuring_probe_name, 1, 2) AS probe " +
//                        "FROM src";
//        tEnv.executeSql(srcExtended);
//    }

    public static void explodeSrcExtended() {
        String srcExploded =
                "CREATE TEMPORARY VIEW src_exploded AS " +
                        "SELECT src_extended.*, " +
                        "TRIM(ip) AS ip " +
                        "FROM src_extended, LATERAL TABLE(split(TRIM(ms_ip_address))) AS T(ip) " +
                        "WHERE TRIM(ip) <> ''";
        tEnv.executeSql(srcExploded);
    }

    public static void createImsiMsisdn() {
        String imsiMsisdn =
                "CREATE TABLE imsi_msisdn (" +
                        "imsi BIGINT," +
                        "msisdn BIGINT" +
                        ") WITH (" +
                        "'connector' = 'jdbc'," +
                        "'url' = '" + config.getString("imsi_msisdn.url") + "'," +
                        "'table-name' = '" + config.getString("imsi_msisdn.dbtable") + "'," +
                        "'username' = '" + config.getString("imsi_msisdn.user") + "'," +
                        "'password' = '" + config.getString("imsi_msisdn.password") + "'" +
                        ")";
        tEnv.executeSql(imsiMsisdn);
    }

    public static void createMsIp() {
        String msIp =
                "CREATE TABLE ms_ip (" +
                        "start_time TIMESTAMP," +
                        "imsi BIGINT," +
                        "msisdn BIGINT," +
                        "ms_ip_address STRING," +
                        "probe STRING" +
                        ") WITH (" +
                        "'connector' = 'jdbc'," +
                        "'url' = '" + config.getString("ms_ip.url") + "'," +
                        "'table-name' = '" + config.getString("ms_ip.dbtable") + "'," +
                        "'username' = '" + config.getString("ms_ip.user") + "'," +
                        "'password' = '" + config.getString("ms_ip.password") + "'" +
                        ")";
        tEnv.executeSql(msIp);
    }

    public static void explodeMsIp() {
        String msIpExploded =
                "CREATE TEMPORARY VIEW ms_ip_exploded AS " +
                        "SELECT " +
                        "ms_ip.imsi AS _imsi," +
                        "ms_ip.msisdn AS _msisdn," +
                        "ms_ip.start_time AS _start_time," +
                        "ms_ip.probe AS _probe," +
                        "TRIM(_ip) AS _ip " +
                        "FROM ms_ip, LATERAL TABLE(split(TRIM(ms_ip_address))) AS T(_ip) " +
                        "WHERE TRIM(_ip) <> ''";
        tEnv.executeSql(msIpExploded);
    }

    public static void joinImsiMsisdn() {
        String joinedImsiMsisdn =
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
                        "WHERE imsi IS NOT NULL ";
//                        + "AND IMSI NOT LIKE '999%"
        tEnv.executeSql(joinedImsiMsisdn);
    }

    public static void joinMsIp() {
        String joinedMsIp =
                "CREATE TEMPORARY VIEW joined_msip AS " +
                        "SELECT * " +
                        "FROM src_exploded " +
                        "JOIN ms_ip_exploded " +
                        "ON (" +
                        "probe = _probe " +
                        "AND " +
                        "ip = _ip " +
                        "AND " +
                        "start_time >= _start_time" +
                        ") " +
                        "WHERE imsi IS NULL ";
//                        + "OR IMSI LIKE '999%"
        tEnv.executeSql(joinedMsIp);
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
