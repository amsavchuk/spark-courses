package com.griddynamics.spark.courses.asavchuk

import org.apache.spark.sql.{Encoder, Encoders, Row}
import org.apache.spark.sql.expressions.Aggregator

class TopChannelAggregator extends Aggregator[Row, Option[TopChannelAggregatorInput],
  TopChannelAggregatorInput] {

  val channelId = "channelId"
  val uniqueSessionsCount = "uniqueSessionsCount"

  private def toTopChannelAggregatorInput(row: Row) = {
    TopChannelAggregatorInput(row.getAs[String](channelId),
      row.getAs[Long](uniqueSessionsCount))
  }

  override def zero: Option[TopChannelAggregatorInput] = None

  override def reduce(acc: Option[TopChannelAggregatorInput], elem: Row): Option[TopChannelAggregatorInput] = {
    acc match {
      case None => Some(toTopChannelAggregatorInput(elem))
      case Some(value) =>
        if (value.uniqueSessionsCount >= elem.getAs[Long](uniqueSessionsCount)) {
          acc
        } else {
          Some(toTopChannelAggregatorInput(elem))
        }
    }
  }

  override def merge(acc1: Option[TopChannelAggregatorInput], acc2: Option[TopChannelAggregatorInput]):
  Option[TopChannelAggregatorInput] = {
    (acc1, acc2) match {
      case (None, None) => None
      case (Some(_), None) => acc1
      case (None, Some(_)) => acc2
      case (Some(value1), Some(value2)) =>
        if (value1.uniqueSessionsCount >= value2.uniqueSessionsCount) {
          acc1
        } else {
          acc2
        }
    }
  }

  override def finish(reduction: Option[TopChannelAggregatorInput]): TopChannelAggregatorInput = reduction.get

  override def bufferEncoder: Encoder[Option[TopChannelAggregatorInput]] =
    Encoders.product[Option[TopChannelAggregatorInput]]

  override def outputEncoder: Encoder[TopChannelAggregatorInput] = Encoders.product[TopChannelAggregatorInput]
}
