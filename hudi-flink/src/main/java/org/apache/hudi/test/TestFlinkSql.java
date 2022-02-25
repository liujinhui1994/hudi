package org.apache.hudi.test;

import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.table.api.StatementSet;
import org.apache.flink.table.api.TableEnvironment;
import org.apache.flink.table.api.bridge.java.StreamTableEnvironment;

/**
 * @author liujinhui
 * @date 2022/2/24 15:32
 * @comment
 */
public class TestFlinkSql {

  public static void main(String[] args) {

    StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
    TableEnvironment tableEnv = StreamTableEnvironment.create(env);

    tableEnv.executeSql("CREATE TABLE t3 (" +
                            "  uuid STRING," +
                            "  name STRING," +
                            "  age INT," +
                            "  ts STRING," +
                            "  `partition` STRING" +
                            ")" +
                            "PARTITIONED BY (`partition`)" +
                            "WITH (" +
                            "  'connector' = 'hudi'," +
                            "  'path' = 'hdfs://127.0.0.1:9000/hudi/test/0225'," +
                            "  'table.type' = 'MERGE_ON_READ'," +
                            "  'write.precombine.field' = 'ts'," +
                            "  'hoodie.datasource.write.recordkey.field' = 'uuid'," +
                            "  'changelog.enabled' = 'true'," +
                            "  'compaction.async.enabled' = 'false'" +
                            ")");

    tableEnv.executeSql("INSERT INTO t3 VALUES" +
                            "  ('id1','Danny',23,'1970-01-01 00:00:01','par1')");

  }
}
