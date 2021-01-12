package com.twitterETL

import com.Utility.UtilityClass
import com.amazonaws.services.s3.model.AmazonS3Exception
import com.awss3.{BucketCreation, S3Configurations}
import org.apache.log4j.Logger
import org.apache.spark.sql.functions.{col, from_json}
import org.apache.spark.sql.types.StructType
import org.apache.spark.sql.{DataFrame, SparkSession}

/***
  * Class reads data from kafka, performs the cleansing  of
  * the data and uploads it into S3
  */
object TwitterETL {
  val sparkSession: SparkSession =
    UtilityClass.createSparkSessionObj("Twitter ETL")
  val logger: Logger = Logger.getLogger(getClass.getName)
  sparkSession.udf.register("removeWords", remove)

  /** *
    * Reads Data From Kafka Topic
    * @param broker String
    * @param topic  String
    * @return DataFrame
    */
  def readDataFromKafka(broker: String, topic: String): DataFrame = {
    logger.info("Reading Data From Kafka Topic")
    try {
      val kafkaDF = sparkSession.readStream
        .format("kafka")
        .option("kafka.bootstrap.servers", broker)
        .option("subscribe", topic)
        .option("startingOffsets", "earliest")
        .load()
      kafkaDF
    } catch {
      case nullPointerException: NullPointerException =>
        logger.error(nullPointerException.printStackTrace())
        throw new Exception("Passed Fields Are Null")
    }
  }

  /** *
    * Extracts Schema From Twitter Sample Json File
    * @param filePath String
    * @return StructType
    */
  def extractSchemaFromTwitterData(filePath: String): StructType = {
    logger.info("Extracting Schema From Twitter Json File")
    try {
      val twitterData = sparkSession.read
        .json(filePath)
        .toDF()
      twitterData.schema
    } catch {
      case nullPointerException: NullPointerException =>
        logger.error(nullPointerException.printStackTrace())
        throw new Exception("Can not create a Path from a null string")
      case fileNotFoundException: org.apache.spark.sql.AnalysisException =>
        logger.error(fileNotFoundException.printStackTrace())
        throw new Exception("Twitter Sample file not exist")
    }
  }

  /** *
    * Casting, Applying  Schema and Selecting Required Columns From The Kafka DataFrame
    * @param kafkaDF DataFrame
    * @param schema  StructType
    * @return DataFrame
    */
  def processKafkaDataFrame(
      kafkaDF: DataFrame,
      schema: StructType
  ): DataFrame = {
    logger.info("Processing The Kafka DataFrame")
    try {
      val twitterStreamDF = kafkaDF
        .selectExpr("CAST(value AS STRING) as jsonData")
        .select(from_json(col("jsonData"), schema).as("data"))
        .select(col("data.retweeted_status") as "tweet")
      val tweetDF =
        twitterStreamDF.select(col("tweet.text") as "tweet_string")
      tweetDF
    } catch {
      case sparkAnalysisException: org.apache.spark.sql.AnalysisException =>
        logger.error(sparkAnalysisException.printStackTrace())
        throw new Exception("Unable to Execute Query")
    }
  }

  /***
    * Interface for saving the content of the streaming Dataset out into external storage
    * @param hashTagDF DataFrame
    * @param pathToSave String
    */
  def writeStreamDataFrame(
      hashTagDF: DataFrame,
      pathToSave: String
  ): Unit = {
    logger.info("Writing the Streaming context DataFrame")
    val query = hashTagDF.writeStream
      .outputMode("append")
      .foreachBatch((outPutDF: DataFrame, batchId: Long) => {
        writeToOutputPath(outPutDF, batchId, pathToSave)
      })
      .start()
    query.awaitTermination()
  }

  /***
    * Writing Data to S3 bucket
    * @param outputDF DataFrame
    * @param batchId Long
    * @param pathToSave String
    */
  def writeToOutputPath(
      outputDF: DataFrame,
      batchId: Long,
      pathToSave: String
  ): Unit = {
    logger.info(
      "Writing the DataFrame into S3 bucket for the batch: " + batchId
    )
    try {
      // Extracting bucket name from URL and checking whether bucket exist or not
      if (pathToSave.startsWith("s3a://")) {
        val startPosition = pathToSave.indexOf("//") + 2
        val lastPosition = pathToSave.lastIndexOf("/")
        val bucketName = pathToSave.substring(startPosition, lastPosition)
        if (!BucketCreation.checkBucketExistsOrNot(bucketName)) {
          BucketCreation.createBucket(bucketName)
        }
      }
      //Saving the output dataframe as csv in the provided path
      outputDF.write
        .mode("append")
        .option("header", value = true)
        .csv(pathToSave)
    } catch {
      case s3Exception: AmazonS3Exception =>
        logger.error(s3Exception.printStackTrace())
        throw new Exception("Unable to write data into aws s3 bucket")
      case nullPointerException: NullPointerException =>
        logger.error(nullPointerException.printStackTrace())
        throw new Exception("null fields passed as a parameter")
    }
  }

  /** *
    * UDF for removing unwanted words from the tweet text field
    * @return String
    */
  def remove: String => String =
    (words: String) => {
      var removeText: String = null
      if (words != null) {
        removeText = words
          .replaceAll("""(\b\w*RT)|[^a-zA-Z0-9\s\.\,\!,\@]""", "")
          .replaceAll("(http\\S+)", "")
          .replaceAll("(@\\w+)", "")
          .replaceAll("(Bla)", "")
          .replaceAll("\\s{2,}", " ")
      } else {
        removeText = "null"
      }
      removeText
    }

  /** *
    * Removing the unwanted words from the tweets data field by applying an UDF
    * @param tweetDF DataFrame
    * @return DataFrame
    */
  def removeUnwantedWords(tweetDF: DataFrame): DataFrame = {
    logger.info("Removing the unwanted words from tweet field")
    try {
      tweetDF.createTempView("remove_words")
      val removedWordsDF = sparkSession.sql(
        """select removeWords(tweet_string) as text from remove_words"""
      )
      val tweetTextDF = removedWordsDF.where("text != 'null'")
      tweetTextDF
    } catch {
      case sparkAnalysisException: org.apache.spark.sql.AnalysisException =>
        logger.error(sparkAnalysisException.printStackTrace())
        throw new Exception("Unable to Execute a Query")
    }
  }

  // Entry Point to the Application
  def main(args: Array[String]): Unit = {
    val broker = args(0)
    val topic = args(1)
    val pathToSave = args(2)
    val kafkaDF = readDataFromKafka(broker, topic)
    val schema = extractSchemaFromTwitterData(
      args(3)
    )
    val tweetDF = processKafkaDataFrame(kafkaDF, schema)
    val removeWordsDF = removeUnwantedWords(tweetDF)
    val awsAccessKey = args(4)
    val awsSecretAccessKey = args(5)
    val status = S3Configurations.connectToS3(
      sparkSession.sparkContext,
      awsAccessKey,
      awsSecretAccessKey
    )
    logger.info("true means connected to s3: " + status)
    writeStreamDataFrame(removeWordsDF, pathToSave)
  }
}
