package com.griddynamics.spark.courses.asavchuk

import org.apache.spark.sql.{DataFrame, SparkSession, functions}
import org.apache.spark.sql.types.{BooleanType, DoubleType, StringType, StructField, StructType, TimestampType}

trait DataFrameEvaluationLike {

  def purchasesFilePath = "src/main/resources/purchases_sample - purchases_sample.tsv"

  def mobileAppClickStreamFilePath =
    "src/main/resources/mobile-app-clickstream_sample - mobile-app-clickstream_sample.tsv"

  val appName = "SparkCapstone"
  val appMaster = "local"

  val headerReadOptionName = "header"
  val headerReadOptionValue = "true"
  val separatorReadOptionName = "sep"
  val separatorReadOptionValue = "\t"
  val timestampFormatReadOptionName = "timestampFormat"
  val timestampFormatReadOptionValue = "yyyy-MM-dd'T'HH:mm:ss"

  val purchaseAggregatorUDAFName = "aggregatePurchases"

  val userId = "userId"
  val eventId = "eventId"
  val eventTime = "eventTime"
  val eventType = "eventType"
  val attributes = "attributes"

  val purchaseId = "purchaseId"
  val purchaseTime = "purchaseTime"
  val billingCost = "billingCost"
  val confirmed = "isConfirmed"

  val mobileAppClickStreamRawTempView = "mobile_app_click_stream_raw"
  val purchasesTempView = "purchases"

  val spark: SparkSession =
    SparkSession
      .builder()
      .appName(appName)
      .master(appMaster)
      .getOrCreate()

  def read(path: String, schema: StructType): DataFrame =
    spark.read.options(Map(
      headerReadOptionName -> headerReadOptionValue,
      separatorReadOptionName -> separatorReadOptionValue,
      timestampFormatReadOptionName -> timestampFormatReadOptionValue))
      .schema(schema)
      .csv(path)

  def setupAndReadFromFiles(): (DataFrame, DataFrame) = {
    spark.udf.register(purchaseAggregatorUDAFName, functions.udaf(new PurchaseAggregator()))

    val mobileAppClickStreamRawSchema: StructType = StructType(Seq(
      StructField(userId, StringType),
      StructField(eventId, StringType),
      StructField(eventTime, TimestampType),
      StructField(eventType, StringType),
      StructField(attributes, StringType)))
    val mobileAppClickStreamRawDF = read(mobileAppClickStreamFilePath, mobileAppClickStreamRawSchema)
    mobileAppClickStreamRawDF.createOrReplaceTempView(mobileAppClickStreamRawTempView)

    val purchasesSchema: StructType = StructType(Seq(
      StructField(purchaseId, StringType),
      StructField(purchaseTime, TimestampType),
      StructField(billingCost, DoubleType),
      StructField(confirmed, BooleanType)))
    val purchasesDF = read(purchasesFilePath, purchasesSchema)
    purchasesDF.createOrReplaceTempView(purchasesTempView)

    (mobileAppClickStreamRawDF, purchasesDF)
  }

  def evaluateResultDataFrame(): DataFrame = {
    spark.sql(
      s"""
         SELECT purchase_campaign.$purchaseId, $purchaseTime, $billingCost, $confirmed, sessionId,
         campaignId, channelId
         FROM (
           SELECT EXPLODE(purchaseIds) AS purchaseId, sessionId, campaignId, channelId
           FROM (
             SELECT purchaseIds, uuid() AS sessionId, campaignId, channelId
             FROM
             (
               SELECT INLINE(aggregatedPurchases.results)
               FROM
               (
                 SELECT $userId, FIRST(aggregatedPurchases) AS aggregatedPurchases
                 FROM
                 (
                   SELECT $userId, $purchaseAggregatorUDAFName(eventType, attributes) OVER
                   (PARTITION BY $userId ORDER BY $eventTime ROWS BETWEEN UNBOUNDED PRECEDING AND UNBOUNDED FOLLOWING)
                   AS aggregatedPurchases
                   FROM
                   (
                     SELECT $userId, $eventId, $eventTime, $eventType, FROM_JSON($attributes, 'map<string, string>')
                       AS $attributes
                     FROM $mobileAppClickStreamRawTempView
                   )
                 )
                 GROUP BY $userId
               )
             )
           )
         ) purchase_campaign
         JOIN purchases ON purchase_campaign.$purchaseId = purchases.$purchaseId
         ORDER BY $purchaseTime
         """)
  }

  def showResultDataFrame(dataFrame: DataFrame): Unit = {
    dataFrame.show(false)
  }
}
