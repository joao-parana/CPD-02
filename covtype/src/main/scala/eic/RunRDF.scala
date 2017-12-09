package eic

import java.util.Calendar

import eic.randomforest.RDFRunner
import org.apache.spark.SparkConf
import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.SparkSession.Builder

object RunRDF {

  var numCores = 0

  val sparkURL = "spark://aldebaran.eic.cefet-rj.br:7077"

  def sleep(t: Long = 2): Unit = {
    Thread.sleep(1000 * t)
  }

  def main(args: Array[String]): Unit = {

    var timeNow = Calendar.getInstance().getTime()
    if (args.length < 1) {
      println("••• |dude, i need at least one parameter. The number of CORES !")
      return
    }
    val cores = args(0)
    numCores = cores.toInt
    if (numCores < 1) {
      println("••• |The number of CORES is less than one")
      return
    }
    if (numCores > 4 && args.length < 3) {
      println("••• |The number of CORES is greather than four")
      return
    }

    var datasetType = "-min"
    var csvFileName = "covtype" + datasetType + ".csv"
    if (args.length > 1 && "full".equals(args(1))) {
      datasetType = "-full"
      csvFileName = "covtype" + datasetType + ".csv"
      println("••• |Running with the full size dataset. csvFileName = " + csvFileName)
    } else {
      println("••• |Running with the small dataset with only 3.000 records. csvFileName = " + csvFileName)
    }

    println(s"••• |The number of CORES is $numCores and now is $timeNow")
    val sparkBuilder: Builder = SparkSession.builder()
    var sparkSession: SparkSession = null

    if (args.length > 2 && "cluster".equals(args(2))) {
      val conf = new SparkConf()
      conf.setMaster(sparkURL)
      conf.setAppName("forest cover type")
      conf.set("spark.cores.max", "16")
      conf.set("spark.submit.deployMode", "cluster")
      // When running on a standalone deploy cluster, the maximum amount of CPU cores to request for the application from across the cluster (not from each machine).
      // If not set, the default will be spark.deploy.defaultCores on Spark's standalone cluster manager.
      conf.set("spark.driver.cores", "3")
      conf.set("spark.driver.memory", "3g")
      conf.set("spark.task.cpus", "1")
      conf.set("spark.local.dir", "/tmp")
      conf.setMaster(sparkURL)
      sparkSession = sparkBuilder.config(conf).getOrCreate()
      sparkSession.sql("SET -v").show(numRows = 200, truncate = false)
      // sparkBuilder.master(sparkURL)      .set("spark.cores.max", "10").appName("forest cover type").getOrCreate()
    } else {
      sparkSession = sparkBuilder.master("local[" + cores + "]").appName("forest cover type").getOrCreate()
    }

    val runRDF = new RDFRunner(sparkSession)
    //sparkSession,
    runRDF.doIt(csvFileName)
  }

}

