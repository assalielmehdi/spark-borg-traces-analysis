import org.apache.spark.sql.{DataFrame, SparkSession}

import java.io.{File, PrintWriter}
import math.{pow, floor}

object Main {
  def main(args: Array[String]): Unit = {
    if (args.length != 5) {
      println("Invalid arguments")
      sys.exit(1)
    }

    val machineEvents = args(0)
    val jobEvents = args(1)
    val taskEvents = args(2)
    val taskUsage = args(3)
    val outPath = args(4)

    val sparkSession = SparkSession.builder
      .master("local")
      .appName("Data Management Project")
      .getOrCreate

    val machineEventsDf = getDfFromGz(sparkSession, machineEvents)
      .toDF("timestamp", "id", "event", "platform", "cpu", "memory")
      .drop("platform")

    computeLostComputePower(machineEventsDf, outPath)
  }

  def getDfFromGz(sparkSession: SparkSession, filePath: String): DataFrame = {
    sparkSession.read.option("inferSchema", "true").csv(filePath)
  }

  def computeLostComputePower(machineEventsDf: DataFrame, outPath: String): Unit = {
    val totalDaysTimeMicro = 29 * 24 * 60 * 60 * 1000 * 1000

    val percentageLostTimeByMachine = machineEventsDf.rdd
      .filter(event => event(2) != 2) // Select only ADD(0) and REMOVE(1) events
      .groupBy(event => event(1))
      .mapValues(events => events.drop(1)) // Remove first event (always ADD(0))
      .mapValues(events => events.take(events.size - events.size % 2)) // If last event if REMOVE(1), drop it
      .mapValues(events => events
        .map(event => toLong(event(0)))
        .sliding(2, 2) // Split to windows of two in the form (REMOVE(1), ADD(0))
        .map(pair => pair.toArray)
        .map(pair => pair(1) - pair(0))
        .sum
      )
      .mapValues(totalLostTime => totalLostTime / totalDaysTimeMicro)

    var avgPercentageLostTime = percentageLostTimeByMachine
      .map(tuple => tuple._2)
      .sum()

    avgPercentageLostTime /= percentageLostTimeByMachine.count()

    val writer = new PrintWriter(new File(outPath + "/lost_compute_power.txt"))
    writer.println(s"\nAverage percentage of lost computational time: ${roundAt(avgPercentageLostTime, 2)} %")
    writer.close()
  }

  def toLong(x: Any): Long = x match {
    case i: Long => i
  }

  def roundAt(n: Double, p: Int): Double = {
    val precision = pow(10, p)
    floor(n * precision) / precision
  }
}

