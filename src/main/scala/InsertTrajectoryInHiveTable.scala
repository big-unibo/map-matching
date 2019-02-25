import org.apache.spark.sql.{SaveMode, SparkSession}
import org.apache.spark.sql.functions._

object InsertTrajectoryInHiveTable {
  def main(args: Array[String]): Unit = {

    val tablename = "trajMilan"
    val spark: SparkSession =
      SparkSession.builder().appName("insertTrajectoryInHiveTable")
        .enableHiveSupport
        .getOrCreate

    import spark.implicits._

    val sc = spark.sparkContext
    // sc.setLogLevel("ERROR")
    // Logger.getLogger("org").setLevel(Level.ERROR)
    // Logger.getLogger("akka").setLevel(Level.ERROR)
    // LogManager.getRootLogger.setLevel(Level.ERROR)

    //val hiveContext = new HiveContext(sc)
    spark.sql("use trajectory")

    val df1913036 = spark.read.format("csv").option("header", "true").option("mode", "DROPMALFORMED").option("inferSchema", true).option("timestampFormat", "yyyy/MM/dd HH:mm:ss").load("hdfs:///user/fvitali/trajMilano/1913036.csv").select("X","Y","time").withColumnRenamed("X","longitude").withColumnRenamed("Y","latitude").withColumn("timestamp", col("time").cast("long").cast("int")).withColumn("customid",lit("custom1")).withColumn("accuracy",lit(10))

    val df2460733 = spark.read.format("csv").option("header", "true").option("mode", "DROPMALFORMED").option("inferSchema", true).option("timestampFormat", "yyyy/MM/dd HH:mm:ss").load("hdfs:///user/fvitali/trajMilano/2460733.csv").select("X","Y","time").withColumnRenamed("X","longitude").withColumnRenamed("Y","latitude").withColumn("timestamp", col("time").cast("long").cast("int")).withColumn("customid",lit("custom2")).withColumn("accuracy",lit(10))

    val df2490763 = spark.read.format("csv").option("header", "true").option("mode", "DROPMALFORMED").option("inferSchema", true).option("timestampFormat", "yyyy/MM/dd HH:mm:ss").load("hdfs:///user/fvitali/trajMilano/2490763.csv").select("X","Y","time").withColumnRenamed("X","longitude").withColumnRenamed("Y","latitude").withColumn("timestamp", col("time").cast("long").cast("int")).withColumn("customid",lit("custom3")).withColumn("accuracy",lit(10))

    val df2565381 = spark.read.format("csv").option("header", "true").option("mode", "DROPMALFORMED").option("inferSchema", true).option("timestampFormat", "yyyy/MM/dd HH:mm:ss").load("hdfs:///user/fvitali/trajMilano/2565381.csv").select("X","Y","time").withColumnRenamed("X","longitude").withColumnRenamed("Y","latitude").withColumn("timestamp", col("time").cast("long").cast("int")).withColumn("customid",lit("custom4")).withColumn("accuracy",lit(10))

    val finalTrajDf = df1913036.union(df2460733).union(df2490763).union(df2565381).write.mode(SaveMode.Append).saveAsTable(tablename)

  }
}