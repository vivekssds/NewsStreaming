package org.news

import org.apache.spark.sql.Dataset
import org.scalatest.{FlatSpec, Matchers, PrivateMethodTester}
import com.typesafe.scalalogging.LazyLogging

abstract class BaseTestUtils
  extends FlatSpec
    with BaseSharedSparkSession
    with Matchers
    with PrivateMethodTester
    with LazyLogging {

  /**
   * Temporary implemented method to compare two datasets for testing purposes
   *
   * @param actualResult - actual result from test
   * @param expectedResult - expected result from test
   * @tparam T - type T
   */

  def assertDatasetsEqual [T] (actualResult: Dataset[T], expectedResult: Dataset[T] ): Unit = {

    val expectedMinusActual = expectedResult.except(actualResult)
    if ( expectedMinusActual.head(1).nonEmpty)
    {
      logger.error("expectedMinusActual is not empty")
      expectedMinusActual.show()
    }

    val actualMinusExpected = actualResult.except(expectedResult)
    if ( actualMinusExpected.head(1).nonEmpty)
    {
      logger.error("actualMinusExpected is not empty")
      actualMinusExpected.show()
    }

    assert(expectedMinusActual.head(1).isEmpty === true && actualMinusExpected.head(1).isEmpty === true)

    assert(actualResult.count == expectedResult.count)

  }
}