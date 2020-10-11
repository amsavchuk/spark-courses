package com.griddynamics.spark.courses.asavchuk

import com.griddynamics.spark.courses.asavchuk.TypeAliases.{PurchaseAggregatorInput, PurchaseId}
import org.apache.spark.sql.{Encoder, Encoders}
import org.apache.spark.sql.expressions.Aggregator

class PurchaseAggregator extends Aggregator[PurchaseAggregatorInput, PurchaseAggregatorIntermediateResults,
  PurchaseAggregatorFinalResults] {

  private val purchaseEventType = "purchase"
  private val appOpenEventType = "app_open"
  private val appCloseEventType = "app_close"

  private val campaignIdAttributeName = "campaign_id"
  private val channelIdAttributeName = "channel_id"
  private val purchaseIdAttributeName = "purchase_id"

  private val unknownCampaignId = "unknownCampaignId"
  private val unknownChannelId = "unknownChannelId"
  private val unknownPurchaseId = "unknownPurchaseId"

  override def zero: PurchaseAggregatorIntermediateResults = PurchaseAggregatorIntermediateResults(Array.empty,
    Array.empty, None, Array.empty)

  override def reduce(accumulator: PurchaseAggregatorIntermediateResults,
                      nextElement: PurchaseAggregatorInput): PurchaseAggregatorIntermediateResults = {
    nextElement._1 match {
      case `purchaseEventType` =>
        val purchaseId = nextElement._2.flatMap(map =>
          map.get(purchaseIdAttributeName)).getOrElse(unknownPurchaseId)
        accumulator.copy(purchaseIdsAtFinish = accumulator.purchaseIdsAtFinish :+ purchaseId)
      case `appOpenEventType` =>
        val availableAttributes = nextElement._2 match {
          case Some(map) =>
            Some(map.getOrElse(campaignIdAttributeName, unknownCampaignId),
              map.getOrElse(channelIdAttributeName, unknownChannelId))
          case _ =>
            Some(unknownCampaignId, unknownChannelId)
        }
        accumulator.copy(appIsOpenedCurrentlyAndAttributesAvailable = availableAttributes)
      case `appCloseEventType` =>
        if (accumulator.purchaseIdsAtFinish.isEmpty) {
          accumulator.copy(appIsOpenedCurrentlyAndAttributesAvailable = None)
        } else {
          if (accumulator.appIsOpenedCurrentlyAndAttributesAvailable.nonEmpty) {
            accumulator.copy(completeResults = accumulator.completeResults :+ PurchaseIdAndCampaignInfo(
                            accumulator.purchaseIdsAtFinish,
                            accumulator.appIsOpenedCurrentlyAndAttributesAvailable.get._1,
                            accumulator.appIsOpenedCurrentlyAndAttributesAvailable.get._2),
              appIsOpenedCurrentlyAndAttributesAvailable = None, purchaseIdsAtFinish = Array.empty)
          } else {
            if (accumulator.completeResults.isEmpty) {
              accumulator.copy(purchaseIdsAtStart = accumulator.purchaseIdsAtFinish, purchaseIdsAtFinish = Array.empty)
            } else {
              accumulator.copy(completeResults = accumulator.completeResults :+ PurchaseIdAndCampaignInfo(
                                accumulator.purchaseIdsAtFinish, unknownCampaignId, unknownChannelId),
                purchaseIdsAtFinish = Array.empty)
            }
          }
        }
      case _ => accumulator
    }
  }

  override def merge(accumulator1: PurchaseAggregatorIntermediateResults,
                     accumulator2: PurchaseAggregatorIntermediateResults): PurchaseAggregatorIntermediateResults = {
    val startPurchaseIds = accumulator1.purchaseIdsAtStart ++ (
      if (accumulator1.completeResults.isEmpty && accumulator1.appIsOpenedCurrentlyAndAttributesAvailable.isEmpty)
        accumulator2.purchaseIdsAtStart else Array.empty[PurchaseId]
      )
    val completeResults = accumulator1.completeResults ++ (
      if (accumulator1.appIsOpenedCurrentlyAndAttributesAvailable.isEmpty ||
        (accumulator1.purchaseIdsAtFinish.isEmpty && accumulator2.purchaseIdsAtStart.isEmpty))
      Array.empty[PurchaseIdAndCampaignInfo] else Array(PurchaseIdAndCampaignInfo(accumulator1.purchaseIdsAtFinish ++
        accumulator2.purchaseIdsAtStart,
        accumulator1.appIsOpenedCurrentlyAndAttributesAvailable.get._1,
        accumulator1.appIsOpenedCurrentlyAndAttributesAvailable.get._2))) ++
      accumulator2.completeResults
    val appIsOpenedCurrentlyAndAttributesAvailable = if (accumulator2.completeResults.isEmpty) {
      if (accumulator2.appIsOpenedCurrentlyAndAttributesAvailable.isEmpty) {
        accumulator1.appIsOpenedCurrentlyAndAttributesAvailable
      } else {
        accumulator2.appIsOpenedCurrentlyAndAttributesAvailable
      }
    } else {
      accumulator2.appIsOpenedCurrentlyAndAttributesAvailable
    }
    val purchaseIdsAtFinish = if (accumulator2.completeResults.isEmpty &&
      accumulator2.appIsOpenedCurrentlyAndAttributesAvailable.isEmpty) {
      accumulator1.purchaseIdsAtFinish ++ accumulator2.purchaseIdsAtFinish
    } else {
      accumulator2.purchaseIdsAtFinish
    }
    PurchaseAggregatorIntermediateResults(startPurchaseIds, completeResults,
      appIsOpenedCurrentlyAndAttributesAvailable, purchaseIdsAtFinish)
  }

  override def finish(reduction: PurchaseAggregatorIntermediateResults): PurchaseAggregatorFinalResults =
    PurchaseAggregatorFinalResults(reduction.completeResults)

  override def bufferEncoder: Encoder[PurchaseAggregatorIntermediateResults] =
    Encoders.product[PurchaseAggregatorIntermediateResults]

  override def outputEncoder: Encoder[PurchaseAggregatorFinalResults] =
    Encoders.product[PurchaseAggregatorFinalResults]
}
