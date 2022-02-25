package org.apache.hudi.test;

import com.alibaba.fastjson.JSONObject;
import org.apache.hudi.common.config.HoodieMetadataConfig;
import org.apache.spark.api.java.JavaSparkContext;
import org.apache.spark.api.java.function.MapFunction;
import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Encoders;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.RowFactory;
import org.apache.spark.sql.SparkSession;
import org.apache.spark.sql.catalyst.encoders.ExpressionEncoder;
import org.apache.spark.sql.catalyst.encoders.RowEncoder;
import org.apache.spark.sql.streaming.OutputMode;
import org.apache.spark.sql.streaming.StreamingQueryException;
import org.apache.spark.sql.streaming.Trigger;
import org.apache.spark.sql.types.DataTypes;
import org.apache.spark.sql.types.Metadata;
import org.apache.spark.sql.types.StructField;
import org.apache.spark.sql.types.StructType;

import java.io.IOException;

/**
 * @author liujinhui
 * @date 2021/1/7 16:09
 * @comment
 */
public class HudiWriteTest {

  public static void main(String[] args) throws IOException, StreamingQueryException {

    SparkSession spark = SparkSession.builder().appName("Hoodie Datasource test")
                             .master("local[2]")
                             .config("spark.serializer", "org.apache.spark.serializer.KryoSerializer")
                             .config("spark.io.compression.codec", "snappy")
                             .config("spark.sql.hive.convertMetastoreParquet", "false")
                             .getOrCreate();
    JavaSparkContext jsc = new JavaSparkContext(spark.sparkContext());

    //String basepath = "hdfs://t3-test-cq-ecs-dtpbu-datalake-cdh-mansger-01:8020/datalake/data/tmp4/";
    //String basepath = "hdfs://127.0.0.1:9000/hudi/test/0728";
    //query(spark, basepath);

    Dataset<String> streamingInput = spark.readStream()
                                         .format("org.apache.spark.sql.kafka010.KafkaSourceProvider")
                                         .option("kafka.bootstrap.servers", "localhost:9092")
                                         .option("startingOffsets", "latest")
                                         .option("maxOffsetsPerTrigger", "100")
                                         .option("subscribe", "test")
                                         .load()
                                         .selectExpr("CAST(value AS STRING)").as(Encoders.STRING());


    ExpressionEncoder<Row> encoder = RowEncoder.apply(getSchema());


    Dataset<Row> dataset = streamingInput.map(new MapFunction<String, Row>() {
      @Override
      public Row call(String s) throws Exception {
        JSONObject jsonObject = JSONObject.parseObject(s);
        int uuid = jsonObject.getInteger("uuid");
        String name = jsonObject.getString("name");
        int code = jsonObject.getInteger("code");
        long ts = jsonObject.getLong("ts");
        String clazz = jsonObject.getString("clazz");
        String type = jsonObject.getString("type");
        return RowFactory.create(uuid, name,code , ts, clazz, type);
      }
    }, encoder);

    write(dataset, "G://tmp/data/");
  }

  public static StructType getSchema() {
    StructField[]fields = new StructField[6];
    fields[0] = new StructField("uuid", DataTypes.IntegerType, true, Metadata.empty());
    fields[1] = new StructField("name", DataTypes.StringType, true, Metadata.empty());
    fields[2] = new StructField("code", DataTypes.IntegerType, true, Metadata.empty());
    fields[3] = new StructField("ts", DataTypes.LongType, true, Metadata.empty());
    fields[4] = new StructField("clazz", DataTypes.StringType, true, Metadata.empty());
    fields[5] = new StructField("type", DataTypes.StringType, true, Metadata.empty());
    return new StructType(fields);
  }


  public static void query(SparkSession spark, String basePath) {
      Dataset<Row> roViewDF = spark.read().format("org.apache.hudi").load(basePath + "/*");
      roViewDF.registerTempTable("hudi_table");
      spark.sql("select * from  hudi_table").show(100,false);
  }

  public static void write(Dataset dataset, String basePath) throws IOException, StreamingQueryException {

    dataset.writeStream().format("org.apache.hudi")
        .option("hoodie.datasource.write.recordkey.field", "uuid")
        .option("hoodie.parquet.small.file.limit", 128 * 1024 * 1024)
        .option("hoodie.deltastreamer.keygen.timebased.timestamp.type", "DATE_STRING")
        .option("hoodie.upsert.shuffle.parallelism", "20")
        .option("hoodie.datasource.write.operation", "insert_overwrite")
        .option("hoodie.datasource.write.precombine.field", "ts")
        .option("hoodie.datasource.write.keygenerator.class", "org.apache.hudi.keygen.ComplexKeyGenerator")
        .option("hoodie.datasource.write.table.type", "MERGE_ON_READ")
        .option("hoodie.embed.timeline.server", false)
        .option("hoodie.cleaner.commits.retained", "2")
        .option("hoodie.keep.min.commits", "4")
        .option("hoodie.keep.max.commits", "5")
        .option("hoodie.table.name", "test")
        .option("hoodie.datasource.write.payload.class","org.apache.hudi.common.model.DefaultHoodieRecordPayload")
        .option("hoodie.finalize.write.parallelism", "20")
        .option("hoodie.rollback.parallelism", "20")
        .option("checkpointLocation", "G://tmp/checkpoint")
        .option(HoodieMetadataConfig.ENABLE.key(), false)
        .outputMode(OutputMode.Append()).trigger(Trigger.ProcessingTime("30 second"))
        .start(basePath).awaitTermination();

  }
}
