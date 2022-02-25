package org.apache.hudi.test;

import org.apache.spark.api.java.JavaSparkContext;
import org.apache.spark.sql.SparkSession;

/**
 * @author liujinhui
 * @date 2022/1/21 10:14
 * @comment
 */
public class Query {

  public static void main(String[] args) {


    SparkSession spark = SparkSession.builder().appName("Hoodie Datasource test")
                             .master("local[2]")
                             .config("spark.serializer", "org.apache.spark.serializer.KryoSerializer")
                             .config("spark.io.compression.codec", "snappy")
                             .config("spark.sql.hive.convertMetastoreParquet", "false")
                             .getOrCreate();
    JavaSparkContext jsc = new JavaSparkContext(spark.sparkContext());

    spark.read().format("org.apache.hudi").load("G://tmp/data");


  }
}
