package org.apache.hudi.test;

import org.apache.spark.api.java.JavaSparkContext;
import org.apache.spark.sql.SparkSession;
import org.apache.spark.sql.hudi.HoodieSparkSessionExtension;

/**
 * @author liujinhui
 * @date 2022/1/24 11:13
 * @comment
 */
public class HudiSQLTest {


  public static void main(String[] args) {

    SparkSession spark = SparkSession.builder().appName("Hoodie Datasource test")
                             .master("local[2]")
                             .withExtensions(new HoodieSparkSessionExtension())
                             .config("spark.serializer", "org.apache.spark.serializer.KryoSerializer")
                             .config("hoodie.insert.shuffle.parallelism", "4")
                             .config("hoodie.upsert.shuffle.parallelism", "4")
                             .config("hoodie.delete.shuffle.parallelism", "4")
                             .getOrCreate();
    JavaSparkContext jsc = new JavaSparkContext(spark.sparkContext());

    //spark.sql("set hoodie.datasource.write.insert.drop.duplicates = true");
    spark.sql(
        "create table tableName2 (" +
            "id int," +
            "name string," +
            "price double," +
            "ts long, " +
            "dt string " +
            ") using hudi " +
            "tblproperties (primaryKey = 'id') " +
            "partitioned by (dt) " +
            "location 'G://tmp/data'");

    spark.sql("insert overwrite table tableName2 select 1 as id, 'a1' as name, 10 as price, 1000 as ts, '2021-01-05' as dt");






  }


}
