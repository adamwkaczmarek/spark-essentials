package part2dataframes

import org.apache.parquet.format.IntType
import org.apache.spark.sql.types.{DoubleType, IntegerType, LongType, StringType, StructField, StructType}
import org.apache.spark.sql.{DataFrame, SparkSession}

object DataFrameBasicsExercises extends App {

  val spark = SparkSession.builder()
    .appName("DataFrameBasicsExercises")
    .config("spark.master", "local")
    .getOrCreate()

  // schema
  val smartPhonesSchema = StructType(Array(
    StructField("Make", StringType),
    StructField("Model", StringType),
    StructField("Screen dimension", DoubleType),
    StructField("Camera megapixel", IntegerType)
  ))

  val smartPhones = Seq(
    ("Samsung", "Galaxy S22", 6.8, 108),
    ("Samsung", "Galaxy S20 FE", 6.5, 12)
  )

  import spark.implicits._
  smartPhones.toDF("Make", "Model", "Screen dimension", "Camera").show()

  private val frame: DataFrame = spark.read.format("json").load("src/main/resources/data/movies.json")
  frame.printSchema()
  println(s"Number of movies ${frame.count()}")

}
