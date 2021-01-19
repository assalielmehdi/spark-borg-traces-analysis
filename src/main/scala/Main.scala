import org.apache.spark.sql.{DataFrame, SparkSession}
import Utils.{getDfFromGz, roundAt, toDouble, toLong}
import org.apache.spark.sql.functions.{avg, count}
import org.apache.spark.mllib.stat.Statistics.corr

object Main {
  /**
   * Q1. What is the distribution of the machines according to their CPU capacity?
   */
  def computeCpuDistribution(
    sparkSession: SparkSession,
    machineEventsDf: DataFrame,
    outPath: String
  ): Unit = {
    val cpuDistribution = machineEventsDf
      .where(machineEventsDf.col("cpu").isNotNull)
      .groupBy("cpu")
      .agg(count("machine_id"))
      .collect()

    sparkSession.sparkContext
      .makeRDD(Array(cpuDistribution.mkString("\n")))
      .saveAsTextFile(s"$outPath/q1")
  }

  /**
   * Q2. What is the percentage of computational power lost due
   * to maintenance (a machine went offline and reconnected later)?
   */
  def computeLostComputePower(
    sparkSession: SparkSession,
    machineEventsDf: DataFrame,
    outPath: String
  ): Unit = {
    val totalDaysTimeMicro = 29 * 24 * 60 * 60 * 1000 * 1000

    val percentageLostTimeByMachine = machineEventsDf.rdd
      .filter(event => event(2) != 2)
      .groupBy(event => event(1))
      .mapValues(events => events.drop(1))
      .mapValues(events => events.take(events.size - events.size % 2))
      .mapValues(events => events
        .map(event => toLong(event(0)))
        .sliding(2, 2)
        .map(pair => pair.toArray)
        .map(pair => pair(1) - pair(0))
        .sum)
      .mapValues(totalLostTime => totalLostTime / totalDaysTimeMicro)

    var avgPercentageLostTime = percentageLostTimeByMachine
      .map(tuple => tuple._2)
      .sum()

    avgPercentageLostTime /= percentageLostTimeByMachine.count()

    val result = s"\nAverage percentage of lost computational time: ${roundAt(avgPercentageLostTime, 2)} %"

    sparkSession.sparkContext
      .makeRDD(Array(result))
      .saveAsTextFile(s"$outPath/q2")
  }

  /**
   * Q3. On average, how many tasks compose a job?
   */
  def computeAvgTasksPerJob(sparkSession: SparkSession, taskEventsDf: DataFrame, outPath: String): Unit = {
    val tasksPerJob = taskEventsDf
      .groupBy("job_id")
      .count()

    val averageTaskPerJob = tasksPerJob
      .agg(avg("count"))
      .first()
      .getDouble(0)

    val result = s"Average number of tasks per job: $averageTaskPerJob"

    sparkSession.sparkContext
      .makeRDD(Array(result))
      .saveAsTextFile(s"$outPath/q3")
  }

  /**
   * Q4. What can you say about the relation between the scheduling class of a job,
   * the scheduling class of its tasks, and their priority?
   */
  def computeScheduleClassPriorityCorrelation(
    sparkSession: SparkSession,
    jobEventsDf: DataFrame,
    taskEventsDf: DataFrame,
    outPath: String
  ): Unit = {
    val scheduleClassPriorityCorrelation = jobEventsDf
      .join(taskEventsDf, jobEventsDf("job_id") === taskEventsDf("job_id"), "inner")
      .groupBy("job_scheduling_class")
      .agg(avg("priority"))
      .collect()

    sparkSession.sparkContext
      .makeRDD(Array(scheduleClassPriorityCorrelation.mkString("\n")))
      .saveAsTextFile(s"$outPath/q4")
  }

  /**
   * Q5. Do tasks with low priority have a higher probability of being evicted?
   */
  def computeEvictedDistribution(
    sparkSession: SparkSession,
    taskEventsDf: DataFrame,
    outPath: String
  ): Unit = {
    val evictedCountByPriority = taskEventsDf
      .where("event_type=2")
      .groupBy("priority")
      .agg(count("*"))
      .collect()

    sparkSession.sparkContext
      .makeRDD(Array(evictedCountByPriority.mkString("\n")))
      .saveAsTextFile(s"$outPath/q5")
  }

  /**
   * Q6. In general, do tasks from the same job run on the same machine?
   */
  def computeTasksMachineDistribution(sparkSession: SparkSession, taskEventsDf: DataFrame, outPath: String): Unit = {
    val machinePerJob = taskEventsDf
      .select("job_id", "machine_id")
      .groupBy("job_id")
      .count()

    val percentageTasksInSameMachine = machinePerJob
      .filter(machinePerJob("count") === 1)
      .count()
      .toDouble / machinePerJob.count().toDouble

    val result = s"Percentage of tasks of the same job running in the same machine: $percentageTasksInSameMachine"

    sparkSession.sparkContext
      .makeRDD(Array(result))
      .saveAsTextFile(s"$outPath/q6")
  }

  /**
   * Q7. Are the tasks that request the more resources the one that consume the more resources?
   */
  def computeTaskRequestUsageCorrelation(
    sparkSession: SparkSession,
    taskEventsDf: DataFrame,
    taskUsageDf: DataFrame,
    outPath: String
  ): Unit = {
    val resourcesDf = taskEventsDf
      .join(taskUsageDf, taskUsageDf("job_id") === taskEventsDf("job_id") && taskUsageDf("task_index") === taskEventsDf("task_index"))
      .select("mean_cpu_usage_rate", "resource_request_cpu_cores", "maximum_memory_usage",
              "resource_request_ram", "mean_local_disk_s pace_used", "resource_request_local_disk_space")
      .where("resource_request_cpu_cores is not null and resource_request_ram is not null and resource_request_local_disk_space is not null")

    val cpu = resourcesDf
      .select("resource_request_cpu_cores").rdd
      .map(row => toDouble(row.get(0)))
    val meanCpuUsageRate = resourcesDf
      .select("mean_cpu_usage_rate").rdd
      .map(row => toDouble(row.get(0)))

    val ram = resourcesDf
      .select("resource_request_ram").rdd
      .map(row => toDouble(row.get(0)))
    val maximumMemoryUsage = resourcesDf
      .select("maximum_memory_usage").rdd
      .map(row => toDouble(row.get(0)))

    val localDiskSpace = resourcesDf
      .select("resource_request_local_disk_space").rdd
      .map(row => toDouble(row.get(0)))
    val meanLocalDiskSpaceUsed = resourcesDf
      .select("mean_local_disk_space_used").rdd
      .map(row => toDouble(row.get(0)))

    val result = List(
      s"Correlation(CPU, Mean CPU Usage Rate) : ${corr(cpu, meanCpuUsageRate)}",
      s"Correlation(RAM, Maximum Memory Usage) : ${corr(ram, maximumMemoryUsage)}",
      s"Correlation(Local Disk Space, Mean Local DIsk Space Used) : ${corr(localDiskSpace, meanLocalDiskSpaceUsed)}"
      )

    sparkSession.sparkContext
      .makeRDD(result)
      .saveAsTextFile(s"$outPath/q7")
  }

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

    sparkSession.sparkContext
      .hadoopConfiguration
      .setBoolean("fs.gs.implicit.dir.repair.enable", false)

    val machineEventsDf = getDfFromGz(sparkSession, machineEvents)
      .toDF("timestamp", "machine_id", "event_type", "platform", "cpu", "memory")

    val jobEventsDf = getDfFromGz(sparkSession, jobEvents)
      .toDF(
        "timestamp", "missing_info", "job_id", "event_type",
        "user_name", "job_scheduling_class", "job_name", "logical_job_name"
        )

    val taskEventsDf = getDfFromGz(sparkSession, taskEvents)
      .toDF(
        "timestamp", "missing_info", "job_id", "task_index", "machine_id",
        "event_type", "user_name", "task_scheduling_class", "priority", "resource_request_cpu_cores",
        "resource_request_ram", "resource_request_local_disk_space", "different_machine_constraint"
        )

    val taskUsageDf = getDfFromGz(sparkSession, taskUsage)
      .toDF("start_time", "end_time", "job_id", "task_index", "machine_id", "mean_cpu_usage_rate",
            "canonical_memory_usage", "assigned_memory_usage", "unmapped_page_cache_memory_usage",
            "total_page_cache_memory_usage", "maximum_memory_usage", "mean_disk_io_time", "mean_local_disk_space_used",
            "maximum_cpu_usage", "maximum_disk_io_time", "cycles_per_instruction", "memory_accesses_per_instruction",
            "sample_portion", "aggregation_type", "sampled_cpu_usage")

    computeCpuDistribution(sparkSession, machineEventsDf, outPath)

    computeLostComputePower(sparkSession, machineEventsDf, outPath)

    computeAvgTasksPerJob(sparkSession, taskEventsDf, outPath)

    computeScheduleClassPriorityCorrelation(sparkSession, jobEventsDf, taskEventsDf, outPath)

    computeEvictedDistribution(sparkSession, taskEventsDf, outPath)

    computeTasksMachineDistribution(sparkSession, taskEventsDf, outPath)

    computeTaskRequestUsageCorrelation(sparkSession, taskEventsDf, taskUsageDf, outPath)

    // TODO: Q8

    // TODO: Q9
  }
}

