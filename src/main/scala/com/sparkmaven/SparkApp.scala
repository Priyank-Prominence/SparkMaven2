package com.sparkmaven

import org.apache.spark.sql.DataFrame
import org.apache.spark.sql.functions.from_json
import org.apache.spark.sql.types._

object SparkApp extends App {
  // Get properties (config.properties file)
  val props = Config.getProperties

  // Create sparkSession
  val sparkSession = Config.createSparkSession
  import sparkSession.implicits._


  //HDFS to Spark to Kafka Process


  sparkSession
    .read.format("json")
    .load(props.getProperty("jsonFile"))
    .selectExpr("to_json(struct(*)) AS value")
    .write.format("kafka")
    .option("kafka.bootstrap.servers", props.getProperty("kafkaBrokerAndPort"))
    .option("topic", props.getProperty("kafkaTopic"))
    .save


  //Kafka to Spark to Kafka process


  val df = sparkSession
    .read.format("kafka")
    .option("kafka.bootstrap.servers", props.getProperty("kafkaBrokerAndPort"))
    .option("subscribe", props.getProperty("kafkaTopic"))
    .option("startingOffsets", "earliest")
    .load()

  val schema = StructType(Seq(
    StructField("cellphone", LongType, nullable = false),
    StructField("firstname", StringType, nullable = false),
    StructField("gender", StringType, nullable = false),
    StructField("id", IntegerType, nullable = false),
    StructField("postaladdress", IntegerType, nullable = false),
    StructField("residentialaddress", StringType, nullable = false),
    StructField("surname", StringType, nullable = false)
  ))

  // Take value column separately into a DataFrame
  val dfValue: DataFrame = df.selectExpr("CAST(value AS STRING)")
    .withColumn("value", from_json($"value", schema))

  val dfCustomer = dfValue.select($"value.id", $"value.firstname", $"value.surname", $"value.gender", $"value.cellphone")
  val dfAddress = dfValue.select($"value.id", $"value.postaladdress", $"value.residentialaddress")

  // Write customer data to customer_topic
  dfCustomer.selectExpr("to_json(struct(*)) AS value")
    .write.format("kafka")
    .option("kafka.bootstrap.servers", props.getProperty("kafkaBrokerAndPort"))
    .option("topic", props.getProperty("customerTopic"))
    .save

  // Write address data to address_topic
  dfAddress.selectExpr("to_json(struct(*)) AS value")
    .write.format("kafka")
    .option("kafka.bootstrap.servers", props.getProperty("kafkaBrokerAndPort"))
    .option("topic", props.getProperty("addressTopic"))
    .save
}
