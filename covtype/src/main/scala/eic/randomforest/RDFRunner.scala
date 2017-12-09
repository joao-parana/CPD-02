package eic.randomforest

import java.util.Calendar

import eic.RunRDF.{numCores, sleep}
import org.apache.spark.ml.classification.{DecisionTreeClassifier, RandomForestClassificationModel, RandomForestClassifier}
import org.apache.spark.ml.evaluation.MulticlassClassificationEvaluator
import org.apache.spark.ml.feature.{VectorAssembler, VectorIndexer}
import org.apache.spark.ml.linalg.Vector
import org.apache.spark.ml.tuning.{ParamGridBuilder, TrainValidationSplit}
import org.apache.spark.ml.{Pipeline, PipelineModel}
import org.apache.spark.mllib.evaluation.MulticlassMetrics
import org.apache.spark.sql.functions._
import org.apache.spark.sql.{DataFrame, SparkSession}

import scala.util.Random

//private val spark: SparkSession
class RDFRunner(protected val spark: SparkSession) {

  def runFromShell(sparkSession: SparkSession, qtyCores: Int = 4, dType: String = "full"): Unit = {
    numCores = qtyCores
    val datasetType = "-" + dType
    var csvFileName = "covtype" + datasetType + ".csv"
    val runRDF = new RDFRunner(spark)
    //sparkSession,
    runRDF.doIt(csvFileName)
  }

  //spark: SparkSession,
  def doIt(csvFileName: String): Unit = {
    import spark.implicits._

    val dataWithoutHeader = spark.read.option("inferSchema", true).option("header", true).csv(csvFileName) // "hdfs:///user/ds/covtype.data"

    println(s"••• |Building column names for Dataset Schema")
    var colNames = Seq(
      "Elevation", "Aspect", "Slope",
      "Horizontal_Distance_To_Hydrology", "Vertical_Distance_To_Hydrology",
      "Horizontal_Distance_To_Roadways",
      "Hillshade_9am", "Hillshade_Noon", "Hillshade_3pm",
      "Horizontal_Distance_To_Fire_Points"
    ) ++ (
      (0 until 4).map(i => s"Wilderness_Area_$i")
      ) ++ (
      (0 until 40).map(i => s"Soil_Type_$i")
      ) ++ Seq("Cover_Type")

    val data = dataWithoutHeader.toDF(colNames: _*).withColumn("Cover_Type", $"Cover_Type".cast("double"))

    println(s"••• |Dataset with ${data.count()} tuples ")
    println(s"••• |Dataset Schema with ${data.columns.length} features ")
    sleep()
    data.printSchema()
    println(s"••• |dataset.show() result")
    sleep()
    data.show()
    // println(s"••• |dataset.head content")
    // data.first()
    // println(s"••• |dataset.head end")
    sleep()
    var ellapsed = 0L

    // Split into 90% train (+ CV), 10% test
    println(s"••• |Split dataset into 80% train, 10% cross-validation and 10% test")
    var now = Calendar.getInstance().getTime().getTime
    val Array(trainData, testData) = data.randomSplit(Array(0.9, 0.1))
    ellapsed = Calendar.getInstance().getTime().getTime - now
    println(s"••• |bench|$numCores|dataset.randomSplit 80,10,10 execution|$ellapsed")

    now = Calendar.getInstance().getTime().getTime
    trainData.cache()
    ellapsed = Calendar.getInstance().getTime().getTime - now
    println(s"••• |bench|$numCores|trainData.cache() execution|$ellapsed")
    println(s"••• |Datasets for training is cached")

    now = Calendar.getInstance().getTime().getTime
    testData.cache()
    ellapsed = Calendar.getInstance().getTime().getTime - now
    println(s"••• |bench|testData.cache() execution|$ellapsed")
    println(s"••• |Datasets for test is cached")

    println(s"••• |Begin Execution")

    // DateTime.now(DateTimeZone.UTC).getMillis()
    if (true) {
      now = Calendar.getInstance().getTime().getTime
      simpleDecisionTree(trainData, testData)
      ellapsed = Calendar.getInstance().getTime().getTime - now
      println(s"••• |bench|$numCores|simpleDecisionTree|$ellapsed")
    }

    val wild = $"wilderness"
    val soil = $"soil"
    if (false) {
      now = Calendar.getInstance().getTime().getTime
      // PARANA TODO: VERIFICAR randomClassifier(trainData, testData)
      ellapsed = Calendar.getInstance().getTime().getTime - now
      println(s"••• |bench|$numCores|randomClassifier|$ellapsed")

      now = Calendar.getInstance().getTime().getTime
      evaluate(trainData, testData)
      ellapsed = Calendar.getInstance().getTime().getTime - now
      println(s"••• |bench|$numCores|evaluate|$ellapsed")

      now = Calendar.getInstance().getTime().getTime

      evaluateCategorical(trainData, testData, wild, soil)
      ellapsed = Calendar.getInstance().getTime().getTime - now
      println(s"••• |bench|$numCores|evaluateCategorical|$ellapsed")
    }

    now = Calendar.getInstance().getTime().getTime
    evaluateForest(trainData, testData, wild, soil)
    ellapsed = Calendar.getInstance().getTime().getTime - now
    println(s"••• |bench|$numCores|evaluateForest|$ellapsed")

    now = Calendar.getInstance().getTime().getTime
    trainData.unpersist()
    ellapsed = Calendar.getInstance().getTime().getTime - now
    println(s"••• |bench|$numCores|trainData.unpersist|$ellapsed")

    now = Calendar.getInstance().getTime().getTime
    testData.unpersist()
    ellapsed = Calendar.getInstance().getTime().getTime - now
    println(s"••• |bench|$numCores|testData.unpersist|$ellapsed")
  }


  def myPrintln(s: Any, desc: String = " "): Unit = {
    println("••• |" + desc + "|" + s)
  }

  def myprintln(s: Any): Unit = {
    println("••• |" + s)
  }

  def simpleDecisionTree(trainData: DataFrame, testData: DataFrame): Unit = {
    import spark.implicits._
    myPrintln("simpleDecisionTree", "simpleDecisionTree execution")
    val inputCols = trainData.columns.filter(_ != "Cover_Type")
    val assembler = new VectorAssembler().
      setInputCols(inputCols).
      setOutputCol("featureVector")

    val assembledTrainData = assembler.transform(trainData)
    assembledTrainData.select("featureVector").show(truncate = false)

    val classifier = new DecisionTreeClassifier().
      setSeed(Random.nextLong()).
      setLabelCol("Cover_Type").
      setFeaturesCol("featureVector").
      setPredictionCol("prediction")

    val model = classifier.fit(assembledTrainData)
    myPrintln(model.toDebugString, "model.toDebugString")

    model.featureImportances.toArray.zip(inputCols).
      sorted.reverse.foreach(myprintln)

    val predictions = model.transform(assembledTrainData)

    predictions.select("Cover_Type", "prediction", "probability").
      show(truncate = false)

    val evaluator = new MulticlassClassificationEvaluator().
      setLabelCol("Cover_Type").
      setPredictionCol("prediction")

    val accuracy = evaluator.setMetricName("accuracy").evaluate(predictions)
    val f1 = evaluator.setMetricName("f1").evaluate(predictions)
    myPrintln(accuracy, "accuracy")
    myPrintln(f1, "evaluate(predictions)")

    val predictionRDD = predictions.
      select("prediction", "Cover_Type").
      as[(Double, Double)].rdd
    val multiclassMetrics = new MulticlassMetrics(predictionRDD)
    myPrintln(multiclassMetrics.confusionMatrix, "multiclassMetrics.confusionMatrix")

    val confusionMatrix = predictions.
      groupBy("Cover_Type").
      pivot("prediction", (1 to 7)).
      count().
      na.fill(0.0).
      orderBy("Cover_Type")

    myPrintln("confusionMatrix.show()", "See below")
    confusionMatrix.show()
  }


  def classProbabilities(data: DataFrame): Array[Double] = {
    import spark.implicits._
    val total = data.count()
    data.groupBy("Cover_Type").count().
      orderBy("Cover_Type").
      select("count").as[Double].
      map(_ / total).
      collect()
  }

  def randomClassifier(trainData: DataFrame, testData: DataFrame): Unit = {
    myPrintln("randomClassifier", "randomClassifier execution")
    val trainPriorProbabilities = classProbabilities(trainData)
    val testPriorProbabilities = classProbabilities(testData)
    val accuracy = trainPriorProbabilities.zip(testPriorProbabilities).map {
      case (trainProb, cvProb) => trainProb * cvProb
    }.sum
    myPrintln(accuracy, "accuracy")
  }

  def evaluate(trainData: DataFrame, testData: DataFrame): Unit = {
    myPrintln("evaluate", "evaluate execution")
    val inputCols = trainData.columns.filter(_ != "Cover_Type")
    val assembler = new VectorAssembler().
      setInputCols(inputCols).
      setOutputCol("featureVector")

    val classifier = new DecisionTreeClassifier().
      setSeed(Random.nextLong()).
      setLabelCol("Cover_Type").
      setFeaturesCol("featureVector").
      setPredictionCol("prediction")

    val pipeline = new Pipeline().setStages(Array(assembler, classifier))

    val paramGrid = new ParamGridBuilder().
      addGrid(classifier.impurity, Seq("gini", "entropy")).
      addGrid(classifier.maxDepth, Seq(1, 20)).
      addGrid(classifier.maxBins, Seq(40, 300)).
      addGrid(classifier.minInfoGain, Seq(0.0, 0.05)).
      build()

    val multiclassEval = new MulticlassClassificationEvaluator().
      setLabelCol("Cover_Type").
      setPredictionCol("prediction").
      setMetricName("accuracy")

    val validator = new TrainValidationSplit().
      setSeed(Random.nextLong()).
      setEstimator(pipeline).
      setEvaluator(multiclassEval).
      setEstimatorParamMaps(paramGrid).
      setTrainRatio(0.9)

    val validatorModel = validator.fit(trainData)

    val paramsAndMetrics = validatorModel.validationMetrics.
      zip(validatorModel.getEstimatorParamMaps).sortBy(-_._1)

    paramsAndMetrics.foreach { case (metric, params) =>
      myPrintln(metric, "validatorModel.validationMetrics • metric")
      myPrintln(params, "validatorModel.validationMetrics • params")
      println()
    }

    val bestModel = validatorModel.bestModel

    myPrintln(bestModel.asInstanceOf[PipelineModel].stages.last.extractParamMap, "ParamMap")

    myPrintln(validatorModel.validationMetrics.max, "max")

    val testAccuracy = multiclassEval.evaluate(bestModel.transform(testData))
    myPrintln(testAccuracy, "testAccuracy")

    val trainAccuracy = multiclassEval.evaluate(bestModel.transform(trainData))
    myPrintln(trainAccuracy, "trainAccuracy")
  }

  def unencodeOneHot(data: DataFrame, wild: org.apache.spark.sql.Column, soil: org.apache.spark.sql.Column): DataFrame = {

    val wildernessCols = (0 until 4).map(i => s"Wilderness_Area_$i").toArray

    val wildernessAssembler = new VectorAssembler().
      setInputCols(wildernessCols).
      setOutputCol("wilderness")

    val unhotUDF = udf((vec: Vector) => vec.toArray.indexOf(1.0).toDouble)

    val withWilderness = wildernessAssembler.transform(data).
      drop(wildernessCols: _*).
      withColumn("wilderness", unhotUDF(wild))

    val soilCols = (0 until 40).map(i => s"Soil_Type_$i").toArray

    val soilAssembler = new VectorAssembler().
      setInputCols(soilCols).
      setOutputCol("soil")

    soilAssembler.transform(withWilderness).
      drop(soilCols: _*).
      withColumn("soil", unhotUDF(soil))
  }

  def evaluateCategorical(trainData: DataFrame, testData: DataFrame, wild: org.apache.spark.sql.Column, soil: org.apache.spark.sql.Column): Unit = {

    myPrintln("evaluateCategorical", "evaluateCategorical execution")
    val unencTrainData = unencodeOneHot(trainData, wild, soil)
    val unencTestData = unencodeOneHot(testData, wild, soil)

    val inputCols = unencTrainData.columns.filter(_ != "Cover_Type")
    val assembler = new VectorAssembler().
      setInputCols(inputCols).
      setOutputCol("featureVector")

    val indexer = new VectorIndexer().
      setMaxCategories(40).
      setInputCol("featureVector").
      setOutputCol("indexedVector")

    val classifier = new DecisionTreeClassifier().
      setSeed(Random.nextLong()).
      setLabelCol("Cover_Type").
      setFeaturesCol("indexedVector").
      setPredictionCol("prediction")

    val pipeline = new Pipeline().setStages(Array(assembler, indexer, classifier))

    val paramGrid = new ParamGridBuilder().
      addGrid(classifier.impurity, Seq("gini", "entropy")).
      addGrid(classifier.maxDepth, Seq(1, 20)).
      addGrid(classifier.maxBins, Seq(40, 300)).
      addGrid(classifier.minInfoGain, Seq(0.0, 0.05)).
      build()

    val multiclassEval = new MulticlassClassificationEvaluator().
      setLabelCol("Cover_Type").
      setPredictionCol("prediction").
      setMetricName("accuracy")

    val validator = new TrainValidationSplit().
      setSeed(Random.nextLong()).
      setEstimator(pipeline).
      setEvaluator(multiclassEval).
      setEstimatorParamMaps(paramGrid).
      setTrainRatio(0.9)

    val validatorModel = validator.fit(unencTrainData)

    val bestModel = validatorModel.bestModel

    myPrintln(bestModel.asInstanceOf[PipelineModel].stages.last.extractParamMap, "ParamMap")

    val testAccuracy = multiclassEval.evaluate(bestModel.transform(unencTestData))
    myPrintln(testAccuracy, "testAccuracy")
  }

  def evaluateForest(trainData: DataFrame, testData: DataFrame, wild: org.apache.spark.sql.Column, soil: org.apache.spark.sql.Column): Unit = {
    myPrintln("evaluateForest", "evaluateForest execution")
    var now = Calendar.getInstance().getTime().getTime
    println(s"••• |timestamp|$now|evaluateForest begin")
    val unencTrainData = unencodeOneHot(trainData, wild, soil)
    val unencTestData = unencodeOneHot(testData, wild, soil)

    val inputCols = unencTrainData.columns.filter(_ != "Cover_Type")
    val assembler = new VectorAssembler().
      setInputCols(inputCols).
      setOutputCol("featureVector")

    val indexer = new VectorIndexer().
      setMaxCategories(40).
      setInputCol("featureVector").
      setOutputCol("indexedVector")

    val classifier = new RandomForestClassifier().
      setSeed(Random.nextLong()).
      setLabelCol("Cover_Type").
      setFeaturesCol("indexedVector").
      setPredictionCol("prediction").
      setImpurity("entropy").
      setMaxDepth(20).
      setMaxBins(300)

    val pipeline = new Pipeline().setStages(Array(assembler, indexer, classifier))

    val paramGrid = new ParamGridBuilder().
      addGrid(classifier.minInfoGain, Seq(0.0, 0.05)).
      addGrid(classifier.numTrees, Seq(1, 10)).
      build()

    val multiclassEval = new MulticlassClassificationEvaluator().
      setLabelCol("Cover_Type").
      setPredictionCol("prediction").
      setMetricName("accuracy")

    val validator = new TrainValidationSplit().
      setSeed(Random.nextLong()).
      setEstimator(pipeline).
      setEvaluator(multiclassEval).
      setEstimatorParamMaps(paramGrid).
      setTrainRatio(0.9)

    val validatorModel = validator.fit(unencTrainData)

    val bestModel = validatorModel.bestModel

    val forestModel = bestModel.asInstanceOf[PipelineModel].
      stages.last.asInstanceOf[RandomForestClassificationModel]

    myPrintln(forestModel.extractParamMap, "ParamMap")
    myPrintln(forestModel.getNumTrees, "forestModel • NumTrees")
    forestModel.featureImportances.toArray.zip(inputCols).
      sorted.reverse.foreach(myprintln)

    val testAccuracy = multiclassEval.evaluate(bestModel.transform(unencTestData))
    myPrintln(testAccuracy, "testAccuracy")

    bestModel.transform(unencTestData.drop("Cover_Type")).select("prediction").show()
    val endTime = Calendar.getInstance().getTime().getTime
    val ellapsed = endTime - now
    println(s"••• |timestamp|$now|$ellapsed|evaluateForest end")
  }

}
