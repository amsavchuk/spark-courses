package com.griddynamics.spark.courses.asavchuk

import org.scalatest.flatspec.AnyFlatSpec
import org.scalatest.matchers.should

class TopChannelAggregatorFinishTests extends AnyFlatSpec with should.Matchers {

  private val channelId1 = "Yandex Ads"

  private val uniqueSessionsCount1 = 1005L

  private val topChannelAggregator = new TopChannelAggregator()

  "Top channel aggregator" should "finish calculation with value from intermediate result" in {
    // Given
    val reduction = Some(TopChannelAggregatorInput(channelId1, uniqueSessionsCount1))

    // When
    val actual = topChannelAggregator.finish(reduction)

    // Then
    val expected = reduction.get

    actual shouldEqual expected
  }
}
