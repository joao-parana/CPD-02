
// importando os implicitos
import spark.implicits._

// Criando o DataFrame porém sem a primeira linha com nomes das colunas. Caracteristica do dataset original.
val dataWithoutHeader = spark.read.
  option("inferSchema", true).
  option("header", true).
  csv("covtype.csv")

// Definindo sequencia com nomes das colunas
val colNames = Seq(
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



val data = dataWithoutHeader.toDF(colNames:_*).
  withColumn("Cover_Type", $"Cover_Type".cast("double"))

