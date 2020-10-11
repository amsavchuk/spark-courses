package com.griddynamics.spark.courses.asavchuk


import com.griddynamics.spark.courses.asavchuk.TypeAliases.{CampaignId, ChannelId, PurchaseId}

// Task 1
case class PurchaseIdAndCampaignInfo(purchaseIds: Array[PurchaseId], campaignId: CampaignId, channelId: ChannelId) {
  // Due to performance reason all element collections in case classes are replaced from List to Array.
  // But Array breaks ability to compare case classes by `==`, so that's why it was introduced overridden
  // method `equals`. It is the best practice to implement `hashcode` along with `equals`, but because
  // `PurchaseIdAndCampaignInfo` is not used as a key in any map, it is left as is for now.
  override def equals(obj: Any): Boolean = {
    if (!obj.isInstanceOf[PurchaseIdAndCampaignInfo]) {
      false
    } else {
      val that = obj.asInstanceOf[PurchaseIdAndCampaignInfo]
      if (this.eq(that)) {
        true
      } else {
        purchaseIds.sameElements(that.purchaseIds) &&
          campaignId == that.campaignId &&
          channelId == channelId
      }
    }
  }
}

case class PurchaseAggregatorIntermediateResults(purchaseIdsAtStart: Array[PurchaseId],
                                                 completeResults: Array[PurchaseIdAndCampaignInfo],
                                                 appIsOpenedCurrentlyAndAttributesAvailable:
                                                 Option[(CampaignId, ChannelId)],
                                                 purchaseIdsAtFinish: Array[PurchaseId]) {
  // See comment above
  override def equals(obj: Any): Boolean = {
    if (!obj.isInstanceOf[PurchaseAggregatorIntermediateResults]) {
      false
    } else {
      val that = obj.asInstanceOf[PurchaseAggregatorIntermediateResults]
      if (this.eq(that)) {
        true
      } else {
        purchaseIdsAtStart.sameElements(that.purchaseIdsAtStart) &&
          completeResults.sameElements(that.completeResults) &&
          appIsOpenedCurrentlyAndAttributesAvailable == that.appIsOpenedCurrentlyAndAttributesAvailable &&
          purchaseIdsAtFinish.sameElements(that.purchaseIdsAtFinish)
      }
    }
  }
}

case class PurchaseAggregatorFinalResults(results: Array[PurchaseIdAndCampaignInfo]) {
  // See comment above
  override def equals(obj: Any): Boolean = {
    if (!obj.isInstanceOf[PurchaseAggregatorFinalResults]) {
      false
    } else {
      val that = obj.asInstanceOf[PurchaseAggregatorFinalResults]
      if (this.eq(that)) {
        true
      } else {
        results.sameElements(that.results)
      }
    }
  }
}

// Task 2
case class TopChannelAggregatorInput(channelId: String, uniqueSessionsCount: Long)
