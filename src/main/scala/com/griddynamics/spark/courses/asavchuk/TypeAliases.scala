package com.griddynamics.spark.courses.asavchuk

object TypeAliases {
  type EventType = String
  type Attributes = Option[Map[String, String]]

  type PurchaseAggregatorInput = (EventType, Attributes)

  type PurchaseId = String
  type CampaignId = String
  type ChannelId = String
}
