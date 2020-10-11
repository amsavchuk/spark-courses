package com.griddynamics.spark.courses.asavchuk

import com.github.mrpowers.spark.fast.tests.DataFrameComparer
import org.apache.spark.sql.DataFrame
import org.mockito.MockitoSugar.{doNothing, doReturn, spy, verify}
import org.mockito.captor.ArgCaptor
import org.scalatest.flatspec.AnyFlatSpec
import org.scalatest.matchers.should

class Task1SolutionIT extends AnyFlatSpec with should.Matchers with DataFrameComparer {

  private val mobileAppClickStreamFilePath = "src/test/resources/it_mobile_app_click_stream.tsv"

  private val purchasesFilePath = "src/test/resources/it_purchases.tsv"

  "Solution of task 1" should "return correct dataframe for data aggregation" in {
    // Given

    // When
    class Task1TestSolution extends Task1SolutionLike {

    }

    val task1Solution = spy[Task1TestSolution](new Task1TestSolution())

    doReturn(mobileAppClickStreamFilePath)
      .when(task1Solution)
      .mobileAppClickStreamFilePath

    doReturn(purchasesFilePath)
      .when(task1Solution)
      .purchasesFilePath

    // we will close SparkSession after test is finished
    doNothing.when(task1Solution).close()

    task1Solution.main(Array.empty)

    // Then
    val resultDataFrameCapture = ArgCaptor[DataFrame]
    verify(task1Solution).showResultDataFrame(resultDataFrameCapture.capture)

    import task1Solution.spark.implicits._

    val rawActual = resultDataFrameCapture.value.cache()
    val actual = rawActual.drop($"sessionId")
    val expected = Seq(
      ("p1", java.sql.Timestamp.valueOf("2019-01-10 0:01:05"), 1000.5, true, "cmp1", "Google Ads"),
      ("p2", java.sql.Timestamp.valueOf("2019-01-10 0:05:05"), 130.0, true, "cmp1", "Google Ads"),
      ("p3", java.sql.Timestamp.valueOf("2019-01-11 18:03:10"), 205.0, true, "cmp2", "Yandex Ads"),
      ("p4", java.sql.Timestamp.valueOf("2019-01-12 12:12:15"), 30.0, false, "cmp2", "Yandex Ads"),
    ).toDF("purchaseId", "purchaseTime", "billingCost", "isConfirmed", "campaignId", "channelId")

    val sessionIds = rawActual.rdd.map(row => row.getAs[String]("sessionId")).collect()

    assertSmallDataFrameEquality(actual, expected, ignoreNullable = true)
    sessionIds(0) shouldEqual sessionIds(1)
    sessionIds(2) shouldEqual sessionIds(3)
    sessionIds(1) shouldNot equal(sessionIds(2))
    task1Solution.spark.close()
  }
}

