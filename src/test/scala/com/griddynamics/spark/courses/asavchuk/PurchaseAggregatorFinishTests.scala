package com.griddynamics.spark.courses.asavchuk

import org.scalatest.flatspec.AnyFlatSpec
import org.scalatest.matchers.should

class PurchaseAggregatorFinishTests extends AnyFlatSpec with should.Matchers {

  private val campaign1 = "campaignN1"
  private val campaign2 = "campaignN2"

  private val channel1 = "channelN1"
  private val channel2 = "channelN2"

  private val purchase1 = "purchaseN1"
  private val purchase2 = "purchaseN2"

  private val purchaseAggregator = new PurchaseAggregator()

  "Purchase aggregator" should "finish calculation with complete purchases list" in {
    // Given
    val reduction = PurchaseAggregatorIntermediateResults(Array.empty, Array(
            PurchaseIdAndCampaignInfo(Array(purchase1), campaign1, channel1),
            PurchaseIdAndCampaignInfo(Array(purchase2), campaign2, channel2)
          ), None, Array.empty)

    // When
    val actual = purchaseAggregator.finish(reduction)

    // Then
    val expected = PurchaseAggregatorFinalResults(reduction.completeResults)
    actual shouldEqual expected
  }
}
