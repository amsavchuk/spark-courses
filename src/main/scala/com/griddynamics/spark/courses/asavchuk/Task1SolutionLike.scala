package com.griddynamics.spark.courses.asavchuk

trait Task1SolutionLike extends DataFrameEvaluationLike with SparkCloseable {
  def main(args: Array[String]): Unit = {
    setupAndReadFromFiles()
    val resultDataFrame = evaluateResultDataFrame()
    showResultDataFrame(resultDataFrame)
    close()
  }

  def close(): Unit = {
    spark.close()
  }
}
