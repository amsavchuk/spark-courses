package com.griddynamics.spark.courses.asavchuk

import com.github.mrpowers.spark.fast.tests.DataFrameComparer
import org.apache.spark.sql.DataFrame
import org.mockito.MockitoSugar.{doNothing, doReturn, spy, times, verify}
import org.mockito.captor.ArgCaptor
import org.scalatest.flatspec.AnyFlatSpec
import org.scalatest.matchers.should

class Task2SolutionIT extends AnyFlatSpec with should.Matchers with DataFrameComparer {

  private val mobileAppClickStreamFilePath = "src/test/resources/it_mobile_app_click_stream.tsv"

  private val purchasesFilePath = "src/test/resources/it_purchases.tsv"

  class Task2TestSolution extends Task2SolutionLike {

  }

  "Solution of task 2" should "return correct dataframe for data aggregation (using plain SQL)" in {
    val task2Solution = spy[Task2TestSolution](new Task2TestSolution())
    doReturn(true)
      .when(task2Solution)
      .usePlainSQL
    testTask2(task2Solution)
  }

  it should "return correct dataframe for data aggregation" in {
    val task2Solution = spy[Task2TestSolution](new Task2TestSolution())
    doReturn(false)
      .when(task2Solution)
      .usePlainSQL
    testTask2(task2Solution)
  }

  private def testTask2(task2Solution: Task2TestSolution): Unit = {
    // Given

    // When
    doReturn(mobileAppClickStreamFilePath)
      .when(task2Solution)
      .mobileAppClickStreamFilePath

    doReturn(purchasesFilePath)
      .when(task2Solution)
      .purchasesFilePath

    // we will close SparkSession after test is finished
    doNothing.when(task2Solution).close()

    task2Solution.main(Array.empty)

    // Then
    val resultDataFrameCapture = ArgCaptor[DataFrame]
    verify(task2Solution, times(2)).showResultDataFrame(resultDataFrameCapture.capture)

    import task2Solution.spark.implicits._

    val (actual1, actual2) = resultDataFrameCapture.values match {
      case v1 :: v2 :: Nil => (v1, v2)
      case _ => fail("DataFrames in showResultDataFrame were not captured correctly")
    }
    val expected1 = Seq(
      ("cmp1", 1130.5),
      ("cmp2", 205.0)
    ).toDF("campaignId", "sumBillingCost")

    val expected2 = Seq(
      ("cmp1", "Facebook Ads", 2L),
      ("cmp2", "Yandex Ads", 1L)
    ).toDF("campaignId", "channelId", "uniqueSessionsCount")

    assertSmallDataFrameEquality(actual1, expected1, ignoreNullable = true)
    assertSmallDataFrameEquality(actual2, expected2, ignoreNullable = true)

    task2Solution.spark.close()
  }
}

