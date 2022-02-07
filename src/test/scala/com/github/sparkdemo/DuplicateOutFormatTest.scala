package com.github.sparkdemo

import org.apache.spark.sql.SparkSession
import org.scalatest.FunSuite

/**
 *
 */
class DuplicateOutFormatTest extends FunSuite {
  val spark = SparkSession.builder()
    .master("local")
    .getOrCreate()
  val sc = spark.sparkContext

  import spark.implicits._

  test("testDuplicateWrite") {
    val data = Array(
      ("k1", "fa", "20210901", 16),
      ("k2", null, "20210902", 15),
      ("k3", "df", "20210903", 14),
      ("k4", null, "20210904", 13)
    )
    val tempDir = System.getProperty("java.io.tmpdir") + "spark-dup-test" + System.nanoTime()
    val df = sc.parallelize(data).toDF("k", "col2", "day", "col4")
    df.write
      .option("format1", "csv")
      .option("format2", "orc")
      .format("dup").save(tempDir)
    df.show(1000, false)
  }

}
