package org.apache.rocketmq;

import org.apache.flink.api.common.accumulators.LongCounter;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.table.api.EnvironmentSettings;
import org.apache.flink.table.api.Table;
import org.apache.flink.table.api.bridge.java.StreamTableEnvironment;

import org.apache.flink.types.Row;
import org.apache.rocketmq.flink.source.util.JsonUtil;

import java.util.Map;


public class Test {


    @org.junit.Test
    public void test1() throws Exception{

        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        env.setParallelism(1);
        EnvironmentSettings settings = EnvironmentSettings.newInstance().useBlinkPlanner().inStreamingMode().build();
        StreamTableEnvironment tbEnv = StreamTableEnvironment.create(env, settings);
        tbEnv.executeSql("CREATE TABLE rocketmq_sink (" +
                "  id int," +
                "  name string," +
                "  c_date timestamp(3)" +
                ") WITH (" +
                "  'connector' = 'rocketmq-x'," +
                "  'topic' = 'TopicTestJson'," +
                "  'producerGroup' = 'test'," +
                "  'nameServerAddress' = 'risen-cdh01:9876'," +
                "  'type' = 'json'"+
                ")");


        tbEnv.executeSql("CREATE TABLE rocketmq_source(" +
                "  id int," +
                "  name varchar," +
                "  c_date string" +
                ") WITH (" +
                "  'connector' = 'rocketmq-x'," +
                "  'topic' = 'Test'," +
                "  'consumerGroup' = 'test'," +
                "  'nameServerAddress' = 'risen-cdh01:9876'," +
                "   'type' = 'json'" +
                ")");


        tbEnv.executeSql("CREATE TABLE source (" +
                "  id int," +
                "  name string," +
                "  c_date date" +
                ") WITH (" +
                " 'connector' = 'jdbc'," +
                " 'url' = 'jdbc:mysql://risen-cdh01:3306/test'," +
                " 'username' = 'flinkx'," +
                " 'password' = 'risen@qazwsx123'," +
                " 'table-name' = 'test')");

        tbEnv.executeSql("CREATE TABLE sink (" +
                "  id int," +
                "  name string," +
                "  c_date timestamp(3)" +
                ") WITH (" +
                " 'connector' = 'jdbc'," +
                " 'url' = 'jdbc:mysql://risen-cdh01:3306/test'," +
                " 'username' = 'flinkx'," +
                " 'password' = 'risen@qazwsx123'," +
                " 'table-name' = 'sinktest')");

        Table table = tbEnv.sqlQuery("select * from rocketmq_source limit 10");
        DataStream<Row> rowDataStream = tbEnv.toAppendStream(table, Row.class);
        rowDataStream.print();
       //TableResult tableResult = tbEnv.executeSql("insert into sink select * from rocketmq_source limit 100");
        //tableResult.print();
        env.execute();
    }

    @org.junit.Test
    public void test2() throws Exception {


    }

}
