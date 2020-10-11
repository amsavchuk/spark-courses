package com.griddynamics.spark.courses.asavchuk

import org.scalatest.flatspec.AnyFlatSpec
import org.scalatest.matchers.should

class PurchaseAggregatorReduceTests extends AnyFlatSpec with should.Matchers {

  private val purchaseEventType = "purchase"
  private val appOpenEventType = "app_open"
  private val appCloseEventType = "app_close"

  private val campaignIdAttributeName = "campaign_id"
  private val channelIdAttributeName = "channel_id"
  private val purchaseIdAttributeName = "purchase_id"

  private val unknownCampaignId = "unknownCampaignId"
  private val unknownChannelId = "unknownChannelId"
  private val unknownPurchaseId = "unknownPurchaseId"

  private val campaign1 = "campaignN1"
  private val campaign2 = "campaignN2"

  private val channel1 = "channelN1"
  private val channel2 = "channelN2"

  private val purchase1 = "purchaseN1"
  private val purchase2 = "purchaseN2"
  private val purchase3 = "purchaseN3"

  private val purchaseAggregator = new PurchaseAggregator()

  "Purchase aggregator" should "reduce simple mobile app click stream events" in {
    // Given
    val data = Seq(
      (appOpenEventType, Some(Map(campaignIdAttributeName -> campaign1, channelIdAttributeName -> channel1))),
      (purchaseEventType, Some(Map(purchaseIdAttributeName -> purchase1))),
      (appCloseEventType, None)
    )
    // When
    val actual = data.foldLeft(purchaseAggregator.zero)((acc, next) => purchaseAggregator.reduce(acc, next))

    // Then
    val expected = PurchaseAggregatorIntermediateResults(Array.empty,
      Array(PurchaseIdAndCampaignInfo(Array(purchase1), campaign1, channel1)), None, Array.empty)

    actual shouldEqual expected
  }

  it should "reduce mobile app click stream events with purchases at start" in {
    // Given
    val data = Seq(
      (purchaseEventType, Some(Map(purchaseIdAttributeName -> purchase1))),
      (purchaseEventType, Some(Map(purchaseIdAttributeName -> purchase2))),
      (appCloseEventType, None),
      (appOpenEventType, Some(Map(campaignIdAttributeName -> campaign1, channelIdAttributeName -> channel1))),
      (purchaseEventType, Some(Map(purchaseIdAttributeName -> purchase3))),
      (appCloseEventType, None)
    )
    // When
    val actual = data.foldLeft(purchaseAggregator.zero)((acc, next) => purchaseAggregator.reduce(acc, next))

    // Then
    val expected = PurchaseAggregatorIntermediateResults(Array(purchase1, purchase2),
      Array(PurchaseIdAndCampaignInfo(Array(purchase3), campaign1, channel1)), None, Array.empty)

    actual shouldEqual expected
  }

  it should "reduce mobile app click stream events with app_open event at finish" in {
    // Given
    val data = Seq(
      (appOpenEventType, Some(Map(campaignIdAttributeName -> campaign1, channelIdAttributeName -> channel1))),
      (purchaseEventType, Some(Map(purchaseIdAttributeName -> purchase1))),
      (appCloseEventType, None),
      (appOpenEventType, Some(Map(campaignIdAttributeName -> campaign2, channelIdAttributeName -> channel2))),
    )
    // When
    val actual = data.foldLeft(purchaseAggregator.zero)((acc, next) => purchaseAggregator.reduce(acc, next))

    // Then
    val expected = PurchaseAggregatorIntermediateResults(Array.empty,
      Array(PurchaseIdAndCampaignInfo(Array(purchase1), campaign1, channel1)), Some(campaign2, channel2),
      Array.empty)

    actual shouldEqual expected
  }

  it should "reduce mobile app click stream events with purchases at finish" in {
    // Given
    val data = Seq(
      (appOpenEventType, Some(Map(campaignIdAttributeName -> campaign1, channelIdAttributeName -> channel1))),
      (purchaseEventType, Some(Map(purchaseIdAttributeName -> purchase1))),
      (appCloseEventType, None),
      (appOpenEventType, Some(Map(campaignIdAttributeName -> campaign2, channelIdAttributeName -> channel2))),
      (purchaseEventType, Some(Map(purchaseIdAttributeName -> purchase2)))
    )
    // When
    val actual = data.foldLeft(purchaseAggregator.zero)((acc, next) => purchaseAggregator.reduce(acc, next))

    // Then
    val expected = PurchaseAggregatorIntermediateResults(Array.empty,
      Array(PurchaseIdAndCampaignInfo(Array(purchase1), campaign1, channel1)), Some(campaign2, channel2),
      Array(purchase2))

    actual shouldEqual expected
  }

  it should "reduce mobile app click stream events with several purchases within one session" in {
    // Given
    val data = Seq(
      (appOpenEventType, Some(Map(campaignIdAttributeName -> campaign1, channelIdAttributeName -> channel1))),
      (purchaseEventType, Some(Map(purchaseIdAttributeName -> purchase1))),
      (purchaseEventType, Some(Map(purchaseIdAttributeName -> purchase2))),
      (appCloseEventType, None)
    )
    // When
    val actual = data.foldLeft(purchaseAggregator.zero)((acc, next) => purchaseAggregator.reduce(acc, next))

    // Then
    val expected = PurchaseAggregatorIntermediateResults(Array.empty, Array(
      PurchaseIdAndCampaignInfo(Array(purchase1, purchase2), campaign1, channel1)
    ), None, Array.empty)

    actual shouldEqual expected
  }

  it should "reduce mobile app click stream events " +
    "with several purchases within different session" in {
    // Given
    val data = Seq(
      (appOpenEventType, Some(Map(campaignIdAttributeName -> campaign1, channelIdAttributeName -> channel1))),
      (purchaseEventType, Some(Map(purchaseIdAttributeName -> purchase1))),
      (appCloseEventType, None),
      (appOpenEventType, Some(Map(campaignIdAttributeName -> campaign2, channelIdAttributeName -> channel2))),
      (purchaseEventType, Some(Map(purchaseIdAttributeName -> purchase2))),
      (appCloseEventType, None)
    )
    // When
    val actual = data.foldLeft(purchaseAggregator.zero)((acc, next) => purchaseAggregator.reduce(acc, next))

    // Then
    val expected = PurchaseAggregatorIntermediateResults(Array.empty, Array(
      PurchaseIdAndCampaignInfo(Array(purchase1), campaign1, channel1),
      PurchaseIdAndCampaignInfo(Array(purchase2), campaign2, channel2)
    ), None, Array.empty)

    actual shouldEqual expected
  }

  it should "reduce simple mobile app click stream events without attributes" in {
    // Given
    val data = Seq(
      (appOpenEventType, None),
      (purchaseEventType, None),
      (appCloseEventType, None)
    )
    // When
    val actual = data.foldLeft(purchaseAggregator.zero)((acc, next) => purchaseAggregator.reduce(acc, next))

    // Then
    val expected = PurchaseAggregatorIntermediateResults(Array.empty,
      Array(PurchaseIdAndCampaignInfo(Array(unknownPurchaseId), unknownCampaignId, unknownChannelId)), None,
      Array.empty)

    actual shouldEqual expected
  }

  it should "reduce simple mobile app click stream events with empty attributes map" in {
    // Given
    val data = Seq(
      (appOpenEventType, Some(Map.empty[String, String])),
      (purchaseEventType, Some(Map.empty[String, String])),
      (appCloseEventType, None)
    )
    // When
    val actual = data.foldLeft(purchaseAggregator.zero)((acc, next) => purchaseAggregator.reduce(acc, next))

    // Then
    val expected = PurchaseAggregatorIntermediateResults(Array.empty,
      Array(PurchaseIdAndCampaignInfo(Array(unknownPurchaseId), unknownCampaignId, unknownChannelId)),
      None, Array.empty)

    actual shouldEqual expected
  }
}
