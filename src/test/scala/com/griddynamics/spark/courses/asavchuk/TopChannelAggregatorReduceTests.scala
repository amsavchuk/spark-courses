package com.griddynamics.spark.courses.asavchuk

import org.apache.spark.sql.catalyst.expressions.GenericRowWithSchema
import org.apache.spark.sql.types.{LongType, StringType, StructField, StructType}
import org.scalatest.flatspec.AnyFlatSpec
import org.scalatest.matchers.should

class TopChannelAggregatorReduceTests extends AnyFlatSpec with should.Matchers {

  private val channelId1 = "Yandex Ads"
  private val channelId2 = "Google Ads"

  private val uniqueSessionsCount1 = 1005L
  private val uniqueSessionsCount2 = 1006L

  private val topChannelAggregator = new TopChannelAggregator()
  private val schema = StructType(Seq(
    StructField("channelId", StringType),
    StructField("uniqueSessionsCount", LongType)
  ))


  "Top channel aggregator" should "reduce records when accumulator is None" in {
    // Given
    val acc = None
    val nextElement = new GenericRowWithSchema(Array(channelId1, uniqueSessionsCount1), schema)

    // When
    val actual = topChannelAggregator.reduce(acc, nextElement)

    // Then
    val expected = Some(TopChannelAggregatorInput(channelId1, uniqueSessionsCount1))

    actual shouldEqual expected
  }

  it should "reduce records when accumulator sessions count equals " +
    "current channelId sessions count" in {
    // Given
    val acc = Some(TopChannelAggregatorInput(channelId2, uniqueSessionsCount1))
    val nextElement = new GenericRowWithSchema(Array(channelId1, uniqueSessionsCount1), schema)

    // When
    val actual = topChannelAggregator.reduce(acc, nextElement)

    // Then
    val expected = Some(TopChannelAggregatorInput(channelId2, uniqueSessionsCount1))

    actual shouldEqual expected
  }

  it should "reduce records when accumulator sessions count is greater than " +
    "current channelId sessions count" in {
    // Given
    val acc = Some(TopChannelAggregatorInput(channelId2, uniqueSessionsCount2))
    val nextElement = new GenericRowWithSchema(Array(channelId1, uniqueSessionsCount1), schema)

    // When
    val actual = topChannelAggregator.reduce(acc, nextElement)

    // Then
    val expected = Some(TopChannelAggregatorInput(channelId2, uniqueSessionsCount2))

    actual shouldEqual expected
  }

  it should "reduce records when accumulator sessions count is less than " +
    "current channelId sessions count" in {
    // Given
    val acc = Some(TopChannelAggregatorInput(channelId2, uniqueSessionsCount1))
    val nextElement = new GenericRowWithSchema(Array(channelId1, uniqueSessionsCount2), schema)

    // When
    val actual = topChannelAggregator.reduce(acc, nextElement)

    // Then
    val expected = Some(TopChannelAggregatorInput(channelId1, uniqueSessionsCount2))

    actual shouldEqual expected
  }
}
