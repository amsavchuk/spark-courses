package com.griddynamics.spark.courses.asavchuk

import java.sql.Timestamp

import com.github.mrpowers.spark.fast.tests.DataFrameComparer
import org.apache.spark.sql.types._
import org.scalatest.flatspec.AnyFlatSpec
import org.scalatest.matchers.should

class Task1SolutionTests extends AnyFlatSpec with should.Matchers with DataFrameComparer
  with DataFrameEvaluationLike {

  private val testFileName = "src/test/resources/test_sample.tsv"

  "File reader" should "read entire csv file into DataFrame" in {
    // Given
    val schema: StructType = StructType(Seq(
      StructField("purchaseId", StringType),
      StructField("purchaseTime", TimestampType),
      StructField("billingCost", DoubleType),
      StructField("isConfirmed", BooleanType)))

    // When
    val actualDataFrame = read(testFileName, schema)

    // Then
    import spark.implicits._
    val expectedDataFrame = Seq(
      ("p1", Timestamp.valueOf("2019-01-01 0:01:05"), 100.5, true),
      ("p2", Timestamp.valueOf("2019-01-01 0:03:10"), 200.0, true),
      ("p3", Timestamp.valueOf("2019-01-01 1:12:15"), 300.0, false),
      ("p4", Timestamp.valueOf("2019-01-01 2:13:05"), 50.2, true),
      ("p5", Timestamp.valueOf("2019-01-01 2:15:05"), 75.0, true),
      ("p6", Timestamp.valueOf("2019-01-02 13:03:00"), 99.0, false)
    ).toDF("purchaseId", "purchaseTime", "billingCost", "isConfirmed")

    assertSmallDataFrameEquality(actualDataFrame, expectedDataFrame, ignoreNullable = true)
  }
}
