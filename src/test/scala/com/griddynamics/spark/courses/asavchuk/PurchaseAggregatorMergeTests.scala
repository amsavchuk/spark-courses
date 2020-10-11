package com.griddynamics.spark.courses.asavchuk

import org.scalatest.flatspec.AnyFlatSpec
import org.scalatest.matchers.should

class PurchaseAggregatorMergeTests extends AnyFlatSpec with should.Matchers {

  private val campaign1 = "campaignN1"
  private val campaign2 = "campaignN2"

  private val channel1 = "channelN1"
  private val channel2 = "channelN2"

  private val purchase1 = "purchaseN1"
  private val purchase2 = "purchaseN2"
  private val purchase3 = "purchaseN3"

  private val purchaseAggregator = new PurchaseAggregator()

  "Purchase aggregator" should "merge two simple mobile app click stream events" in {
    // Given
    val intermediateResults1 =
      PurchaseAggregatorIntermediateResults(Array.empty, Array(
              PurchaseIdAndCampaignInfo(Array(purchase1), campaign1, channel1),
              PurchaseIdAndCampaignInfo(Array(purchase2), campaign2, channel2)), None, Array.empty)

    val intermediateResults2 =
      PurchaseAggregatorIntermediateResults(Array.empty, Array(
              PurchaseIdAndCampaignInfo(Array(purchase3), campaign1, channel2),
              PurchaseIdAndCampaignInfo(Array(purchase1), campaign2, channel1)), None, Array.empty)

    // When
    val actual = purchaseAggregator.merge(intermediateResults1, intermediateResults2)

    // Then
    val expected = PurchaseAggregatorIntermediateResults(Array.empty, Array(
          PurchaseIdAndCampaignInfo(Array(purchase1), campaign1, channel1),
          PurchaseIdAndCampaignInfo(Array(purchase2), campaign2, channel2),
          PurchaseIdAndCampaignInfo(Array(purchase3), campaign1, channel2),
          PurchaseIdAndCampaignInfo(Array(purchase1), campaign2, channel1)), None, Array.empty)

    actual shouldEqual expected
  }

  it should "merge two simple mobile app click stream events with multiple purchases" in {
    // Given
    val intermediateResults1 =
      PurchaseAggregatorIntermediateResults(Array.empty, Array(
              PurchaseIdAndCampaignInfo(Array(purchase1, purchase2), campaign1, channel1)), None, Array.empty)

    val intermediateResults2 =
      PurchaseAggregatorIntermediateResults(Array.empty, Array(
              PurchaseIdAndCampaignInfo(Array(purchase3, purchase1), campaign2, channel2)), None, Array.empty)

    // When
    val actual = purchaseAggregator.merge(intermediateResults1, intermediateResults2)

    // Then
    val expected = PurchaseAggregatorIntermediateResults(Array.empty, Array(
          PurchaseIdAndCampaignInfo(Array(purchase1, purchase2), campaign1, channel1),
          PurchaseIdAndCampaignInfo(Array(purchase3, purchase1), campaign2, channel2)), None, Array.empty)

    actual shouldEqual expected
  }

  it should "merge two mobile app click stream events with intermediate purchases" in {
    // Given
    val intermediateResults1 =
      PurchaseAggregatorIntermediateResults(Array.empty, Array(
              PurchaseIdAndCampaignInfo(Array(purchase1), campaign1, channel1),
              PurchaseIdAndCampaignInfo(Array(purchase2), campaign2, channel2)), Some(campaign1, channel2),
        Array(purchase3))

    val intermediateResults2 =
      PurchaseAggregatorIntermediateResults(Array(purchase3), Array(
              PurchaseIdAndCampaignInfo(Array(purchase1), campaign2, channel1)), None, Array.empty)

    // When
    val actual = purchaseAggregator.merge(intermediateResults1, intermediateResults2)

    // Then
    val expected = PurchaseAggregatorIntermediateResults(Array.empty, Array(
          PurchaseIdAndCampaignInfo(Array(purchase1), campaign1, channel1),
          PurchaseIdAndCampaignInfo(Array(purchase2), campaign2, channel2),
          PurchaseIdAndCampaignInfo(Array(purchase3, purchase3), campaign1, channel2),
          PurchaseIdAndCampaignInfo(Array(purchase1), campaign2, channel1)), None, Array.empty)

    actual shouldEqual expected
  }

  it should "merge two mobile app click stream events with start and finish data" in {
    // Given
    val intermediateResults1 =
      PurchaseAggregatorIntermediateResults(Array(purchase1), Array(
              PurchaseIdAndCampaignInfo(Array(purchase2), campaign2, channel2)), None, Array.empty)

    val intermediateResults2 =
      PurchaseAggregatorIntermediateResults(Array.empty, Array(
              PurchaseIdAndCampaignInfo(Array(purchase3), campaign1, channel2)), Some(campaign2, channel1),
        Array(purchase1))

    // When
    val actual = purchaseAggregator.merge(intermediateResults1, intermediateResults2)

    // Then
    val expected = PurchaseAggregatorIntermediateResults(Array(purchase1), Array(
          PurchaseIdAndCampaignInfo(Array(purchase2), campaign2, channel2),
          PurchaseIdAndCampaignInfo(Array(purchase3), campaign1, channel2)), Some(campaign2, channel1),
      Array(purchase1))

    actual shouldEqual expected
  }

  it should "merge two mobile app click stream events without purchases within defined sessions" in {
    // Given
    val intermediateResults1 =
      PurchaseAggregatorIntermediateResults(Array(purchase1, purchase2), Array.empty, None, Array.empty)

    val intermediateResults2 =
      PurchaseAggregatorIntermediateResults(Array(purchase3), Array.empty, None, Array.empty)

    // When
    val actual = purchaseAggregator.merge(intermediateResults1, intermediateResults2)

    // Then
    val expected = PurchaseAggregatorIntermediateResults(Array(purchase1, purchase2, purchase3),
      Array.empty, None, Array.empty)

    actual shouldEqual expected
  }

  it should "merge two mobile app click stream events without purchases within defined sessions at finish" in {
    // Given
    val intermediateResults1 =
      PurchaseAggregatorIntermediateResults(Array.empty, Array.empty, None, Array(purchase1, purchase2))

    val intermediateResults2 =
      PurchaseAggregatorIntermediateResults(Array.empty, Array.empty, None, Array(purchase3))

    // When
    val actual = purchaseAggregator.merge(intermediateResults1, intermediateResults2)

    // Then
    val expected = PurchaseAggregatorIntermediateResults(Array.empty, Array.empty, None,
      Array(purchase1, purchase2, purchase3))

    actual shouldEqual expected
  }
}
