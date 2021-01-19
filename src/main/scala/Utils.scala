import org.apache.spark.sql.{DataFrame, SparkSession}

import scala.math.{floor, pow}

object Utils {
  def getDfFromGz(sparkSession: SparkSession, filePath: String): DataFrame = {
    sparkSession.read.csv(filePath)
  }

  def toLong(x: Any): Long = x match {
    case arg: Long => arg
    case arg: String => arg.toLong
  }

  def toDouble(x: Any): Double = x match {
    case arg: Double => arg
    case arg: String => arg.toDouble
  }

  def roundAt(n: Double, p: Int): Double = {
    val precision = pow(10, p)
    floor(n * precision) / precision
  }
}
