import org.apache.spark.sql.SparkSession

object Main {
  def main(args: Array[String]): Unit = {
    if (args.length != 3) {
      println("Invalid arguments")
      sys.exit(1)
    }

    val jobLogs = args(1)
    val machineLogs = args(2)

    val spark = SparkSession.builder
      .master("local")
      .appName("Data Management Project")
      .getOrCreate

    val jobLogsDf = spark.read.option("header", "true").csv(jobLogs)
    jobLogsDf.foreach(row => println(row))

    val machineLogsDf = spark.read.option("header", "true").csv(machineLogs)
    machineLogsDf.foreach(row => println(row))
  }

}