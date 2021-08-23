package com.sparkmaven

import java.io.FileNotFoundException
import java.util.Properties

import org.apache.spark.sql.SparkSession

object Config {
  val props: Properties = getProperties
  def getProperties: Properties = {
    val propertiesFile = "config.properties"
    val input = getClass.getClassLoader.getResourceAsStream(propertiesFile)

    val properties = new Properties()
    if (input != null) properties.load(input)
    else throw new FileNotFoundException(s"""Properties file $propertiesFile does not exist.""")

    properties
  }

  def createSparkSession: SparkSession = {
    val session = SparkSession.builder
      .appName(props.getProperty("appName"))
      .master(props.getProperty("master"))
      .config("fs.defaultFS", props.getProperty("defaultFs"))
      .getOrCreate

    session
  }
}
