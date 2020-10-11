package com.griddynamics.spark.courses.asavchuk

import org.apache.spark.sql.catalyst.encoders.RowEncoder
import org.apache.spark.sql.types._
import org.apache.spark.sql.{DataFrame, functions}

trait Task2SolutionLike extends DataFrameEvaluationLike with SparkCloseable {

  import org.apache.spark.sql.functions._
  import spark.implicits._

  val purchaseCampaignTempView = "purchase_campaign"

  val topChannelAggregatorUDAFName = "aggregateTopChannel"

  val topCampaignsCount = 10

  def usePlainSQL: Boolean = true

  def calculateTopCampaignsPlainSQL(): DataFrame = {
    spark.sql(
      s"""
              SELECT campaignId, sumBillingCost
              FROM
              (
                SELECT campaignId, SUM(billingCost) AS sumBillingCost
                FROM $purchaseCampaignTempView
                WHERE isConfirmed
                GROUP BY campaignId
              )
              ORDER BY sumBillingCost DESC
              LIMIT $topCampaignsCount
    """)
  }

  def calculateTopCampaigns(mobileAppClickStreamRawDF: DataFrame): DataFrame = {
    mobileAppClickStreamRawDF
      .where($"isConfirmed" === true)
      .groupBy($"campaignId")
      .agg(sum($"billingCost").alias("sumBillingCost"))
      .orderBy($"sumBillingCost".desc)
      .limit(topCampaignsCount)
  }

  def calculateTopChannelInEachCampaignPlainSQL(): DataFrame = {
    val inputEncoder = RowEncoder(StructType(
      Seq(
        StructField("campaignId", StringType),
        StructField("channelId", StringType),
        StructField("uniqueSessionsCount", LongType)
      )
    ))
    spark.udf.register(topChannelAggregatorUDAFName, functions.udaf(new TopChannelAggregator(), inputEncoder))
    spark.sql(
      """
        SELECT campaignId, topChannelTuple.channelId AS channelId,
          topChannelTuple.uniqueSessionsCount AS uniqueSessionsCount
        FROM
        (
          SELECT campaignId, aggregateTopChannel(*) AS topChannelTuple
          FROM
          (
            SELECT campaignId, channelId, COUNT(*) AS uniqueSessionsCount
            FROM
            (
              SELECT ELEMENT_AT(attributes, 'campaign_id') AS campaignId,
                ELEMENT_AT(attributes, 'channel_id') AS channelId
              FROM
              (
                SELECT FROM_JSON(attributes, 'map<string, string>') AS attributes
                FROM mobile_app_click_stream_raw
                WHERE eventType = 'app_open'
              )
            )
            GROUP BY campaignId, channelId
          )
          GROUP BY campaignId
        )
        ORDER BY campaignId
      """
    )
  }

  def calculateTopChannelInEachCampaign(mobileAppClickStreamRawDF: DataFrame): DataFrame = {
    val topChannelAggregator = new TopChannelAggregator().toColumn
    mobileAppClickStreamRawDF
      .where($"eventType" === "app_open")
      .select(from_json($"attributes", MapType(StringType, StringType)).alias("attributes"))
      .groupBy(
        $"attributes".getItem("campaign_id").alias("campaignId"),
        $"attributes".getItem("channel_id").alias("channelId"))
      .agg(count($"attributes").alias("uniqueSessionsCount"))
      .groupBy($"campaignId")
      .agg(topChannelAggregator.alias("topChannelTuple"))
      .select($"campaignId", $"topChannelTuple.channelId".alias("channelId"),
        $"topChannelTuple.uniqueSessionsCount".alias("uniqueSessionsCount"))
      .orderBy($"campaignId")
  }

  def calculateTopCampaignsCaller(dataFrame: DataFrame): DataFrame = {
    if (usePlainSQL) {
      calculateTopCampaignsPlainSQL()
    } else {
      calculateTopCampaigns(dataFrame)
    }
  }

  def calculateTopChannelInEachCampaignCaller(dataFrame: DataFrame): DataFrame = {
    if (usePlainSQL) {
      calculateTopChannelInEachCampaignPlainSQL()
    } else {
      calculateTopChannelInEachCampaign(dataFrame)
    }
  }

  def main(args: Array[String]): Unit = {
    val (mobileAppClickStreamRawDF, _) = setupAndReadFromFiles()
    val jointDataFrame = evaluateResultDataFrame()
    jointDataFrame.createOrReplaceTempView(purchaseCampaignTempView)

    // Task 2.1
    val topCampaignsDataFrame = calculateTopCampaignsCaller(jointDataFrame)
    showResultDataFrame(topCampaignsDataFrame)

    // Task 2.2
    val topChannelsDataFrame = calculateTopChannelInEachCampaignCaller(mobileAppClickStreamRawDF)
    showResultDataFrame(topChannelsDataFrame)

    close()
  }

  override def close(): Unit = {
    spark.close()
  }
}
