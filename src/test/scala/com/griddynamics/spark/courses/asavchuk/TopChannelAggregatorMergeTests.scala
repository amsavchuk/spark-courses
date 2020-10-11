package com.griddynamics.spark.courses.asavchuk

import org.scalatest.flatspec.AnyFlatSpec
import org.scalatest.matchers.should

class TopChannelAggregatorMergeTests extends AnyFlatSpec with should.Matchers {

  private val channelId1 = "Yandex Ads"
  private val channelId2 = "Google Ads"

  private val uniqueSessionsCount1 = 1005L
  private val uniqueSessionsCount2 = 1006L

  private val topChannelAggregator = new TopChannelAggregator()

  "Top channel aggregator" should "merge two records with different unique sessions count" in {
    // Given
    val acc1 = Some(TopChannelAggregatorInput(channelId1, uniqueSessionsCount1))
    val acc2 = Some(TopChannelAggregatorInput(channelId2, uniqueSessionsCount2))

    // When
    val actual = topChannelAggregator.merge(acc1, acc2)

    // Then
    actual shouldEqual acc2
  }

  it should "merge two records with the same unique sessions count" in {
    // Given
    val acc1 = Some(TopChannelAggregatorInput(channelId1, uniqueSessionsCount1))
    val acc2 = Some(TopChannelAggregatorInput(channelId2, uniqueSessionsCount1))

    // When
    val actual = topChannelAggregator.merge(acc1, acc2)

    // Then

    actual shouldEqual acc1
  }
}
