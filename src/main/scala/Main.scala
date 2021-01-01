import org.apache.spark.sql.SparkSession

object Main {
  def main(args: Array[String]): Unit = {
    if (args.length != 5) {
      println("Invalid arguments")
      sys.exit(1)
    }

    val machineEvents = args(1)
    val jobEvents = args(2)
    val taskEvents = args(3)
    val taskUsage = args(4)


    val spark = SparkSession.builder
      .master("local")
      .appName("Data Management Project")
      .getOrCreate

    val machineLogsDf = spark.read.option("header", "true").csv(machineEvents)
    machineLogsDf.foreach(row => println(row))
  }

}