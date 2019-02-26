import org.apache.log4j.{Level, Logger}
import org.apache.spark.serializer.KryoSerializer
import org.apache.spark.sql._
import org.datasyslab.geosparksql.utils.GeoSparkSQLRegistrator
import org.apache.spark.sql.functions.udf
import org.apache.spark.sql.functions._
import org.datasyslab.geospark.serde.GeoSparkKryoRegistrator
import org.osgeo.proj4j.{CRSFactory, CoordinateTransformFactory, ProjCoordinate}
import scala.collection.mutable.ListBuffer
import org.apache.spark.sql.expressions.Window

object EntryPoint extends App {
  {

    Logger.getLogger("org").setLevel(Level.WARN)
    Logger.getLogger("akka").setLevel(Level.WARN)

    val numExecutor = 10
    val numCores = 6
    val numRepartition = numExecutor * numCores * 3

    if (args.length < 5) {
      throw new IllegalArgumentException("Not enough parameters. Example usage: alpha=4 beta=10 tau=100 theta=4 gamma=200")
    }

    val PAR_ALPHA = args(0).split("=")(1).toInt
    val PAR_BETA = args(1).split("=")(1).toInt
    val PAR_TAU = args(2).split("=")(1).toInt
    val PAR_THETA = args(3).split("=")(1).toInt
    val PAR_GAMMA = args(4).split("=")(1)
    val PAR_TOP50 = args.length == 6

    val spark = SparkSession.builder
      .config("spark.memory.offHeap.enabled", value = true)
      .config("spark.memory.offHeap.size", "16g")
      .config("spark.serializer", classOf[KryoSerializer].getName)
      .config("spark.kryo.registrator", classOf[GeoSparkKryoRegistrator].getName)
      .config("geospark.global.index", "true")
      .config("geospark.join.gridtype", "rtree")
      .appName("MapMatching")
      .enableHiveSupport
      .getOrCreate

    GeoSparkSQLRegistrator.registerAll(spark.sqlContext)
    import spark.implicits._

    val outputTableName = "MapMatchedTrajectories"
    spark.sql("drop table if exists " + outputTableName)

    //take tables from openStreetMap structures
    val osmWaysDF = spark.sql("select * from osmwaysmilan").withColumnRenamed("id", "wayID").withColumnRenamed("tags", "featureTags").withColumnRenamed("nodes", "nodesList")
    val osmNodesDF = spark.sql("select * from osmnodesmilan")

    //filter from ways table to take only the real roads
    val onlyHighwayThatRepresentRoads = List("motorway", "trunk", "primary", "secondary", "tertiary", "unclassified", "residential")
    val onlyRoadsDF = osmWaysDF.where(col("featureTags")("highway").isNotNull).filter(col("featureTags")("highway").isin(onlyHighwayThatRepresentRoads: _*)) //prendo solo le strade vere

    //explode list nodes column of road table
    val explodeNodes = onlyRoadsDF.select(onlyRoadsDF("*"), posexplode(onlyRoadsDF("nodesList")))

    //substitude node list with real GPS cords list with Join on Node table
    val joinNodesToTakeCordsDF = explodeNodes.join(osmNodesDF, explodeNodes("col") === osmNodesDF("id")).select(explodeNodes("wayID"), explodeNodes("timestamp"), explodeNodes("version"), explodeNodes("changesetid"), explodeNodes("featureTags"), osmNodesDF("latitude"), osmNodesDF("longitude"), explodeNodes("pos").as("pointPosition"))
    val concatenateCoordinates = udf((first: String, second: String) => Seq(first.toDouble, second.toDouble))
    val unifiedCordsDF = joinNodesToTakeCordsDF.select(joinNodesToTakeCordsDF("wayID"), joinNodesToTakeCordsDF("timestamp"), joinNodesToTakeCordsDF("version"), joinNodesToTakeCordsDF("changesetid"), joinNodesToTakeCordsDF("featureTags"), joinNodesToTakeCordsDF("latitude"), joinNodesToTakeCordsDF("longitude"), joinNodesToTakeCordsDF("pointPosition")).withColumn("coordinates", concatenateCoordinates(joinNodesToTakeCordsDF("longitude"), joinNodesToTakeCordsDF("latitude"))).orderBy(col("wayID"), col("pointPosition"))
    val regroupWayCordsDF = unifiedCordsDF.groupBy("wayID", "timestamp", "version", "changesetid").agg(collect_list("coordinates").as("coordinates")).withColumnRenamed("wayID", "roadID")
    //udf to calculate real lenght of each road in dataset
    val calcLenght = udf((coordList: Seq[Seq[Double]]) => DistanceCalculatorImpl().calculateDistanceInMeter(Location(coordList.head.last, coordList.head.head), Location(coordList.last.last, coordList.last.head)))
    val roadDF = regroupWayCordsDF.join(onlyRoadsDF, regroupWayCordsDF("roadID") === onlyRoadsDF("wayID")).withColumn("lenght", calcLenght(col("coordinates"))).select(regroupWayCordsDF("*"), onlyRoadsDF("featureTags"), col("lenght"), onlyRoadsDF("nodesList"))
    val roadFirstAndLastPoint = regroupWayCordsDF.withColumn("initialPoint", col("coordinates").getItem(0)).withColumn("lastPoint", col("coordinates").apply(size(col("coordinates")).minus(1))).select(col("roadID"), col("initialPoint"), col("lastPoint"), col("coordinates"))

    //create a map to maintain foreach segment initial end ending point -> needs of that inside Viterbi to calculate orthogonal projection
    val segInFinMap: Map[String, ((Double, Double), (Double, Double))] = roadFirstAndLastPoint.collect().map(r => {
      val initialPoint: Seq[Double] = r.getAs[Seq[Double]]("initialPoint")
      val lastPoint: Seq[Double] = r.getAs[Seq[Double]]("lastPoint")
      r.getAs[String]("roadID") -> ((initialPoint.last, initialPoint.head), (lastPoint.last, lastPoint.head))
    }).toMap
    //map broadcast, - 25MB
    val bsegInFinMap = spark.sparkContext.broadcast(segInFinMap)

    //creation of adjacency table between road segments
    val explodeNodesAfterLenghtCalc = roadDF.withColumn("nodes", explode(onlyRoadsDF("nodesList"))).select(col("roadID"), col("nodes"), col("lenght"))
    val firstJoin = explodeNodesAfterLenghtCalc.as("waySottoOsservazione").join(explodeNodesAfterLenghtCalc.as("wayAttigua"), col("waySottoOsservazione.nodes") === col("wayAttigua.nodes") && col("waySottoOsservazione.roadID") =!= col("wayAttigua.roadID")).select(col("waySottoOsservazione.roadID"), col("waySottoOsservazione.lenght"), col("wayAttigua.roadID").as("roadIDAttigua"), col("wayAttigua.lenght").as("wayAttiguaLenght"))
    //firstJoin.cache()
    firstJoin.createOrReplaceTempView("firstJoin")
    //distribute by + cache table to speed up next multi-join beetween same table
    val dfDist = spark.sql("SELECT * FROM firstJoin DISTRIBUTE BY roadID,roadIDAttigua").repartition(col("roadID"), col("roadIDAttigua"))
    dfDist.createOrReplaceTempView("completeNeighborhood")
    spark.sql("CACHE TABLE completeNeighborhood")
    val groupWay = firstJoin.groupBy(col("roadID"), col("lenght")).agg(collect_list(map(col("roadIDAttigua"), col("wayAttiguaLenght"))).as("stradeAttigue"))


    /**
      * metodo per calcolare la query di join per calcolo path vicinato
      *
      * method used to generate multi-join query used to generate neighborhood foeach road segment
      *
      * @param neighborhoodTable table of segments neighborhood of candidate segments after k-means
      * @param tableName         table of segments neighborhood of deep 1 pre-calculated
      * @param deep              deep of search of segments
      * @param theresold         max lenght of summed segments inside a neighborhood
      * @return string query
      */
    def createOmegaLeftJoin(neighborhoodTable: String, tableName: String, deep: Int, theresold: String): String = {
      val chars = 'a' to 'z'
      var selectString = "SELECT "
      val fromString = "FROM " + neighborhoodTable + " "
      var leftOuterString = ""
      val listChars = neighborhoodTable :: chars.toList
      for (i <- 0 until deep) {
        if (i == deep - 1) {
          selectString = selectString.concat(listChars(i) + ".roadID AS " + "roadID" + listChars(i) + ", " + listChars(i) + ".lenght AS " + "lenght" + listChars(i) + ", " + listChars(i) + ".roadIDAttigua AS " + "roadIDAttigua" + listChars(i) + ", " + listChars(i) + ".wayAttiguaLenght AS " + "wayAttiguaLenght" + listChars(i) + " ")
        }
        else selectString = selectString.concat(listChars(i) + ".roadID AS " + "roadID" + listChars(i) + ", " + listChars(i) + ".lenght AS " + "lenght" + listChars(i) + ", ")
        if (i == deep - 1) {
          val tempSubCondition = listChars.take(i).map(_ + ".roadID != " + listChars(i) + ".roadID ").mkString(" AND ")
          val sumCondition = listChars.take(i).map(_ + ".lenght").mkString("+")
          val additionalEndCondition = listChars.take(i).map(_ + ".roadID != " + listChars(i) + ".roadIDAttigua ").mkString(" AND ")
          leftOuterString = leftOuterString.concat("LEFT OUTER JOIN " + tableName + " " + listChars(i) + " ON " + listChars(i - 1) + ".roadIDAttigua = " + listChars(i) + ".roadID AND " + tempSubCondition + " AND " + additionalEndCondition + " AND (" + sumCondition + ") < " + theresold + " ")
        }
        else if (i != 0 && i < deep - 1) {
          val tempSubCondition = listChars.take(i).map(_ + ".roadID != " + listChars(i) + ".roadID ").mkString(" AND ")
          val sumCondition = listChars.take(i).map(_ + ".lenght").mkString("+")
          leftOuterString = leftOuterString.concat("LEFT OUTER JOIN " + tableName + " " + listChars(i) + " ON " + listChars(i - 1) + ".roadIDAttigua = " + listChars(i) + ".roadID AND " + tempSubCondition + " AND (" + sumCondition + ") < " + theresold + " ")
        }

      }
      selectString.concat(fromString.concat(leftOuterString))
    }


    //GEOJson struct creation to create GEOSpark Geometry column
    val roadDFnew = roadDF.withColumnRenamed("roadID", "firstWay").withColumnRenamed("lenght", "roadLenght")
    val joinOnlyRoad = roadDFnew.join(groupWay, roadDFnew("firstWay") === groupWay("roadID"))
    val monoStructCreateCorrectStruct = joinOnlyRoad.select(struct(lit("Feature").as("type"), col("roadID").as("id"), struct(col("timestamp"), col("version"), col("changesetid").as("changeset"), col("lenght"), col("featureTags.highway").as("highway"), col("featureTags.loc_ref").as("loc_ref"), col("featureTags.name").as("name"), col("featureTags.oneway").as("oneway"), col("featureTags.surface").as("surface")).alias("properties"), struct(lit("LineString").as("type"), col("coordinates")).alias("geometry")).alias("toJson")).withColumn("json", to_json(col("toJson")))
    monoStructCreateCorrectStruct.createOrReplaceTempView("roadDF")

    //creates Geospark geometry column
    var geoRoadDF = spark.sql(
      """
        |SELECT ST_GeomFromGeoJSON(json) AS geoElement, toJson.id AS roadID, COALESCE(toJson.properties.name , "noName") AS roadName, toJson.properties.lenght AS lenght
        |FROM roadDF
      """.stripMargin)
    geoRoadDF.createOrReplaceTempView("geoRoadDF")

    //coords conversion to meter based
    var convGeoRoadDF = spark.sql(
      """
        |SELECT ST_Transform(geoElement, "epsg:4326", "epsg:3857") AS geoElement, roadID, roadName,lenght
        |FROM geoRoadDF
      """.stripMargin)
    convGeoRoadDF.createOrReplaceTempView("convGeoRoadDF")

    //creation of trajectories dataset -- IMPORTANT check that table name corresponds to hive trajectory dataset!
    val trajectoryDF = spark.sql("select * from trajectories")
    val fiterTrajDF = trajectoryDF.select("customid", "trajid", "timest", "latitude", "longitude", "timediff", "spacediff", "speed", "geojson")
    val trajectoryMilanSpeedDF = fiterTrajDF.where(col("speed") > 0)
    trajectoryMilanSpeedDF.createOrReplaceTempView("trajTablePreFilter")

    //filter trajectory dataset to take only trajectory with special features
    //used to take only trajectory with average speed of 25kh\h and with at least 25 point
    val trajFilteredDF = spark.sql("select * from(" +
      "select *, max(pointOrder) over(partition by customid,trajid) trajPointCount from (select *, avg(spacediff) over(partition by customid,trajid) spaceAvg, avg(timediff) over(partition by customid,trajid) timeAvg," +
      " avg(speed) over(partition by customid,trajid) speedAvg," +
      "  ROW_NUMBER() OVER (PARTITION BY customid,trajid ORDER BY timest) AS pointOrder from trajTablePreFilter) a " +
      " where spaceAvg>0.01 and speedAvg > 25) b " +
      "where b.trajPointCount >25")
    trajFilteredDF.createOrReplaceTempView("trajFiltered")

    var tabTraj = "trajFiltered"
    if (PAR_TOP50) {
      //dataframe con traiettoria - punti dentro al centro
      val trajInsideMilanCenter = spark.sql(
        "select * " +
          "from (" +
          "select a.customid,a.trajid, count(a.*) as numPointOfTrajInside " +
          "from (" +
          "select * " +
          "from trajFiltered " +
          "where latitude between 45.4370 and 45.5020 and longitude between 9.1344 and 9.2344" +
          ") a " +
          "group by a.customid,a.trajid" +
          ") b " +
          "order by b.numPointOfTrajInside DESC " +
          "limit 50")
      //trajInsideMilanCenter.cache()
      //trajInsideMilanCenter.count()
      //trajInsideMilanCenter.show(false)
      trajInsideMilanCenter.createOrReplaceTempView("top50trajPointInsideMilanCenter")

      val top20TrajComplete = spark.sql(
        "select trajFiltered.* " +
          "from trajFiltered JOIN top50trajPointInsideMilanCenter ON (trajFiltered.customid = top50trajPointInsideMilanCenter.customid AND trajFiltered.trajid = top50trajPointInsideMilanCenter.trajid)")
      val trajToWrite = top20TrajComplete.select("customid", "trajid", "latitude", "longitude", "timest")
      top20TrajComplete.createOrReplaceTempView("top50traj")

      tabTraj = "top50traj"
    }

    //same procedure done with road segments
    val explodetrajConverted = spark.sql(
      ("""
         |SELECT ST_GeomFromGeoJSON(geojson) AS geoPoint, latitude,longitude,customid,trajid,timest
         |FROM """ + tabTraj +
        """
        """).stripMargin)
    explodetrajConverted.createOrReplaceTempView("geoTrajFiltered")

    val convExplodetrajConverted = spark.sql(
      """
        |SELECT ST_Transform(geoPoint, "epsg:4326", "epsg:3857") AS geoPoint, latitude,longitude,customid,trajid,timest
        |FROM geoTrajFiltered
      """.stripMargin)
    convExplodetrajConverted.createOrReplaceTempView("convGeoTrajFiltered")

    //query used to calculate K-means road segment foreach traj point with Viterbi's trasmission probability calculation based on normalized distance
    val queryDF = spark.sql(
      ("""SELECT c.roadID,c.customid,c.trajid,c.timest,c.latitude,c.longitude, c.invertedDist/c.sumDist AS prob
         |FROM (
         |    SELECT b.roadID,b.customid,b.trajid,b.timest,b.latitude,b.longitude, 1/b.distance AS invertedDist,
         |        ROW_NUMBER() OVER (PARTITION BY b.customid, b.trajid, b.timest ORDER BY b.distance) AS ranko,
         |        SUM(1/b.distance) OVER(PARTITION BY b.customid, b.trajid, b.timest) AS sumDist
         |    FROM (
         |        SELECT gr.roadID,tf.customid,tf.trajid,tf.timest,tf.latitude,tf.longitude, CASE WHEN ST_Distance(tf.geoPoint, gr.geoElement) > 5 THEN ST_Distance(tf.geoPoint, gr.geoElement) ELSE 5 END AS distance
         |        FROM convGeoRoadDF gr JOIN convGeoTrajFiltered tf ON ST_Distance(tf.geoPoint, gr.geoElement) <= """ + PAR_TAU +
        """
          |    ) b
          |) c
          |WHERE c.ranko <= """ + PAR_ALPHA +
        """ """).stripMargin).cache()


    //speed up calculation of segment's neighborhood using only useful segments
    val segmentRoadInterest = queryDF.select(col("roadID")).distinct().repartition(numRepartition)
    val segmentNeighborhood = dfDist.join(segmentRoadInterest, firstJoin("roadID") === segmentRoadInterest("roadID")).select(firstJoin("*"))
    val deepSearch = PAR_THETA
    val distanceThereshold = PAR_GAMMA
    segmentNeighborhood.createOrReplaceTempView("segsSelectedNeigh")

    //compute neighborhood
    val createPaths = spark.sql(createOmegaLeftJoin("completeNeighborhood", "completeNeighborhood", deepSearch, distanceThereshold)).cache()

    //take each sub path generated by previous query
    val allPossibilities = createPaths.rdd.map(row => {
      var listPoss: List[List[(String, Double)]] = List()
      var temp: List[(String, Double)] = List()
      for (i <- 0 until (deepSearch + 1) * 2 by 2) {
        if (row.get(i).asInstanceOf[String] != null) {
          temp = temp :+ (row.get(i).asInstanceOf[String], row.get(i + 1).asInstanceOf[Double])
          listPoss = listPoss :+ temp
        }
      }
      listPoss
    })
    //explode rdd and create dataframe of paths given start and end segments
    val flatAllPos = allPossibilities.flatMap(list => list).map(x => ((x.head._1, x.last._1), x)).aggregateByKey(List[(String, Double)](("de", 100000)))((elementPalo, element) => if (element.unzip._2.sum < elementPalo.unzip._2.sum) element else elementPalo, (elementPartizOne, elementPartizTwo) => if (elementPartizOne.unzip._2.sum < elementPartizTwo.unzip._2.sum) elementPartizOne else elementPartizTwo)
    val segAdjacencyMatrix = flatAllPos.map(x => (x._1._1, x._1._2, x._2)).toDF("startID", "endID", "path").repartition(numRepartition, col("startID"), col("endID"))

    /*
     //you can choose to use this code to collect to driver the adjacency matrix and broadcast it -
     //USE THAT ONLY WITH LOW GAMMA PARAMETER!
     val adjacencyMap:Map[(String,String),List[(String,Double)]] = flatAllPos.collect().map(r=> {r._1 -> r._2}).toMap
     //val adjacencyMap = flatAllPos.collect().map(r=> {r._1._1 -> Map(r._1._2->r._2)}).toMap
     //println(adjacencyMap)
     //adjacencyMap.foreach(x=>println(x))
     val bstateProbabilitiesMatrix = spark.sparkContext.broadcast(adjacencyMap)
    */

    //join k-means dataframe with adjacency matrix
    val incorporateRoadInfo = queryDF.withColumn("roadSelected", struct("roadID", "prob"))
    val groupByTrajectoryPoint = incorporateRoadInfo.groupBy(col("customid"), col("trajid"), col("timest"), col("latitude"), col("longitude")).agg(collect_list("roadSelected").as("roadsCandidateForPoint"))
    val addToEachPointSegmentOfNextPoint = groupByTrajectoryPoint.withColumn("nextSegIDs", lead(col("roadsCandidateForPoint"), 1).over(Window.partitionBy("customid", "trajid").orderBy("timest")))
    val explodeRoadPossibilities = addToEachPointSegmentOfNextPoint.rdd.flatMap(row => {
      val tmpOne = row.getAs[Seq[Row]]("roadsCandidateForPoint")
      val tmpTwo = row.getAs[Seq[Row]]("nextSegIDs")
      if (tmpTwo == null) {
        tmpOne.map(x => (row.getAs[String]("customid"), row.getAs[Int]("trajid"), row.getAs[Int]("timest"), row.getAs[Double]("latitude"), row.getAs[Double]("longitude"), x.getAs[String]("roadID"), x.getAs[Double]("prob"), null))
      } else tmpOne.flatMap(x => tmpTwo.map(y => (row.getAs[String]("customid"), row.getAs[Int]("trajid"), row.getAs[Int]("timest"), row.getAs[Double]("latitude"), row.getAs[Double]("longitude"), x.getAs[String]("roadID"), x.getAs[Double]("prob"), y.getAs[String]("roadID"))))
    }).toDF("customid", "trajid", "timest", "latitude", "longitude", "roadID", "prob", "succID")
    val joinPathInfoToEachRoadPair = explodeRoadPossibilities.join(segAdjacencyMatrix, explodeRoadPossibilities("roadID") === segAdjacencyMatrix("startID") && explodeRoadPossibilities("succID") === segAdjacencyMatrix("endID"), "left")

    //regroup infomations of each trajectory
    val incorporateRoadNewInfo = joinPathInfoToEachRoadPair.withColumn("roadSelected", struct("roadID", "prob", "succID", "path"))
    val groupByTrajectorySecondTime = incorporateRoadNewInfo.groupBy(col("customid"), col("trajid"), col("timest"), col("latitude"), col("longitude")).agg(collect_list("roadSelected").as("roadsCandidateForPoint"))
    val incorporateAllPointInfoToTraj = groupByTrajectorySecondTime.withColumn("pointWithRoads", struct("timest", "latitude", "longitude", "roadsCandidateForPoint"))
    val allTrajectoryWithAllInfoDF = incorporateAllPointInfoToTraj.groupBy(col("customid"), col("trajid"))
      .agg(sort_array(collect_list("pointWithRoads"))
        .as("trajectoryWithRoads"))
      .cache()


    //creates geographical converters used inside Map-matching procedure
    val csName1 = "EPSG:4326"
    val csName2 = "EPSG:3857"
    val ctFactory = new CoordinateTransformFactory()
    val csFactory = new CRSFactory()
    val crs1 = csFactory.createFromName(csName1)
    val crs2 = csFactory.createFromName(csName2)
    val transTo3857 = ctFactory.createTransform(crs1, crs2)
    val transTo4326 = ctFactory.createTransform(crs2, crs1)
    val converterTo3857 = spark.sparkContext.broadcast(transTo3857)
    val converterTo4326 = spark.sparkContext.broadcast(transTo4326)


    /**
      * Method used to updated road path lenght, with orthogonal projection of points
      *
      * @param listPath   segments list
      * @param startPoint start point of path
      * @param endPoint   end point of path
      * @return updated lenght of path
      */
    def newSegPathLenght(listPath: List[(String, Double)], startPoint: (Double, Double), endPoint: (Double, Double)): Double = {
      //converts point and check projection in road segments
      var newSegPathLenght: Double = 0.0
      val initialSegCoords: ((Double, Double), (Double, Double)) = bsegInFinMap.value(listPath.head._1)
      val finalSegCoords: ((Double, Double), (Double, Double)) = bsegInFinMap.value(listPath.last._1)
      val pointOnFirstSegmentOfPath = orthogonalProj(new ProjCoordinate(initialSegCoords._1._1, initialSegCoords._1._2),
        new ProjCoordinate(initialSegCoords._2._1, initialSegCoords._2._2),
        new ProjCoordinate(startPoint._1, startPoint._2))
      val pointOnLastSegmentOfPath = orthogonalProj(new ProjCoordinate(finalSegCoords._1._1, finalSegCoords._1._2),
        new ProjCoordinate(finalSegCoords._2._1, finalSegCoords._2._2),
        new ProjCoordinate(endPoint._1, endPoint._2))
      //if both are not orthogonal, mantain listPath sum of segments
      if (pointOnFirstSegmentOfPath.isEmpty && pointOnLastSegmentOfPath.isEmpty) newSegPathLenght = listPath.unzip._2.sum
      //caso un solo segmento, devo capire che estremo del segmento considerare per calcolare la distanza con il punto proiettato ortonogonalmente
      //if only one of points is orthogonal, update lenght of his segment
      else if (listPath.length == 1) {
        if (pointOnFirstSegmentOfPath.isEmpty) {
          if (DistanceCalculatorImpl().calculateDistanceInMeter(Location(startPoint._1, startPoint._2), Location(initialSegCoords._1._1, initialSegCoords._1._2)) < DistanceCalculatorImpl().calculateDistanceInMeter(Location(startPoint._1, startPoint._2), Location(initialSegCoords._2._1, initialSegCoords._2._2))) {
            newSegPathLenght = DistanceCalculatorImpl().calculateDistanceInMeter(Location(initialSegCoords._1._1, initialSegCoords._1._2), Location(pointOnLastSegmentOfPath.get.lat, pointOnLastSegmentOfPath.get.lon))
          } else newSegPathLenght = DistanceCalculatorImpl().calculateDistanceInMeter(Location(initialSegCoords._2._1, initialSegCoords._2._2), Location(pointOnLastSegmentOfPath.get.lat, pointOnLastSegmentOfPath.get.lon))
        } else if (pointOnLastSegmentOfPath.isEmpty) {
          if (DistanceCalculatorImpl().calculateDistanceInMeter(Location(endPoint._1, endPoint._2), Location(finalSegCoords._1._1, finalSegCoords._1._2)) < DistanceCalculatorImpl().calculateDistanceInMeter(Location(endPoint._1, endPoint._2), Location(finalSegCoords._2._1, finalSegCoords._2._2))) {
            newSegPathLenght = DistanceCalculatorImpl().calculateDistanceInMeter(Location(finalSegCoords._1._1, finalSegCoords._1._2), Location(pointOnFirstSegmentOfPath.get.lat, pointOnFirstSegmentOfPath.get.lon))
          } else newSegPathLenght = DistanceCalculatorImpl().calculateDistanceInMeter(Location(finalSegCoords._2._1, finalSegCoords._2._2), Location(pointOnFirstSegmentOfPath.get.lat, pointOnFirstSegmentOfPath.get.lon))
        } else {
          newSegPathLenght = DistanceCalculatorImpl().calculateDistanceInMeter(pointOnFirstSegmentOfPath.get, pointOnLastSegmentOfPath.get)
        }
      }
      else {
        var newListPath = listPath.unzip._2
        if (pointOnFirstSegmentOfPath.isDefined) {
          var newLenghtFirstSeg: Double = 0.0
          if (initialSegCoords._1 == bsegInFinMap.value(listPath(1)._1)._1 || initialSegCoords._1 == bsegInFinMap.value(listPath(1)._1)._2) {
            newLenghtFirstSeg = DistanceCalculatorImpl().calculateDistanceInMeter(Location(initialSegCoords._1._1, initialSegCoords._1._2), Location(pointOnFirstSegmentOfPath.get.lat, pointOnFirstSegmentOfPath.get.lon))
          } else {
            newLenghtFirstSeg = DistanceCalculatorImpl().calculateDistanceInMeter(Location(initialSegCoords._2._1, initialSegCoords._2._2), Location(pointOnFirstSegmentOfPath.get.lat, pointOnFirstSegmentOfPath.get.lon))
          }
          newListPath = newListPath.updated(0, newLenghtFirstSeg)
        }
        if (pointOnLastSegmentOfPath.isDefined) {
          var newLenghtLastSeg: Double = 0.0
          if (finalSegCoords._1 == bsegInFinMap.value(listPath(listPath.length - 2)._1)._1 || finalSegCoords._1 == bsegInFinMap.value(listPath(listPath.length - 2)._1)._2) {
            newLenghtLastSeg = DistanceCalculatorImpl().calculateDistanceInMeter(Location(finalSegCoords._1._1, finalSegCoords._1._2), Location(pointOnLastSegmentOfPath.get.lat, pointOnLastSegmentOfPath.get.lon))
          } else {
            newLenghtLastSeg = DistanceCalculatorImpl().calculateDistanceInMeter(Location(finalSegCoords._2._1, finalSegCoords._2._2), Location(pointOnLastSegmentOfPath.get.lat, pointOnLastSegmentOfPath.get.lon))
          }
          newListPath = newListPath.updated(newListPath.length - 1, newLenghtLastSeg)
        }
        newSegPathLenght = newListPath.sum
      }
      newSegPathLenght
    }

    //metodo che calcola la proiezione ortogonale punto segmento e successivamente la lunghezza punto nuovo sul segmento/segmento
    /**
      * method used to calculate orthogonal projection of a point on a road segment
      * @param initialPointLine first point of road segment
      * @param finalPointLine last point of road segment
      * @param point check projection of this point
      * @return return an Option that include coordinates of projection if the point is orthogonal, otherwise empty
      */
    def orthogonalProj(initialPointLine: ProjCoordinate, finalPointLine: ProjCoordinate, point: ProjCoordinate): Option[Location] = {
      val convertedInitialPointLine = new ProjCoordinate()
      converterTo3857.value.transform(initialPointLine, convertedInitialPointLine)
      val convertedFinalPointLine = new ProjCoordinate()
      converterTo3857.value.transform(finalPointLine, convertedFinalPointLine)
      val convertedPoint = new ProjCoordinate()
      converterTo3857.value.transform(point, convertedPoint)
      var resultOrth = DistanceCalculatorImpl().pointOnLine(Location(convertedInitialPointLine.x, convertedInitialPointLine.y), Location(convertedFinalPointLine.x, convertedFinalPointLine.y), Location(convertedPoint.x, convertedPoint.y))
      if (resultOrth.isDefined) {
        val convertResult = new ProjCoordinate()
        converterTo4326.value.transform(new ProjCoordinate(resultOrth.get.lat, resultOrth.get.lon), convertResult)
        resultOrth = Option(Location(convertResult.x, convertResult.y))
      }
      resultOrth
    }

    //forach trajectory (row of rdd composed by customid,trajectoryid,group of points(groups of candidate roads))
    val resutlViterbis = allTrajectoryWithAllInfoDF.rdd.flatMap(z => {
      var emissionStateMatrix: Map[String, Map[String, Double]] = Map()
      var initialStateDistribution: Map[String, Double] = Map()
      var stateMatrix: Map[Int, ListBuffer[String]] = Map()
      var observations = ListBuffer[String]()
      var obsNum = ListBuffer[Int]()
      var obsMap = ListBuffer[(Double, Double)]()
      var adjacencyMap: Map[(String, String), List[(String, Double)]] = Map()

      //here extract all the useful info
      val customid: String = z.getAs[String]("customid")
      val trajid: Int = z.getAs[Int]("trajid")
      var numberOfPointTrajCounter = 0
      val trajectoryWithCandidateRoads: Seq[Row] = z.getAs[Seq[Row]]("trajectoryWithRoads")
      val parametroRumoreGPS = 10
      trajectoryWithCandidateRoads.foreach(x => {
        val timest: Int = x.getAs[Int]("timest")
        val latitude: Double = x.getAs[Double]("latitude")
        val longitude: Double = x.getAs[Double]("longitude")
        var roadListForObservation = ListBuffer[String]()
        observations += customid + trajid + timest
        obsMap += ((latitude, longitude))
        obsNum += timest
        x.getAs[Seq[Row]]("roadsCandidateForPoint").foreach(y => {
          val roadID: String = y.getAs[String]("roadID")
          val succID: String = y.getAs[String]("succID")
          val rowpath = y.getAs[Seq[Row]]("path")
          if (rowpath != null) {
            val path = rowpath.map(x => (x.getAs[String]("_1"), x.getAs[Double]("_2")))
            adjacencyMap += ((roadID, succID) -> path.toList)
          }
          val prob: Double = y.getAs[Double]("prob")
          //CALCOLO PROBABILITà PAPER 2009
          //val prob = (1/(Math.sqrt(2*math.Pi)*parametroRumoreGPS))*Math.pow(Math.E,-0.5*Math.pow(y.getAs[Double]("distance")/parametroRumoreGPS,2))
          roadListForObservation += roadID
          if (emissionStateMatrix.contains(roadID)) {
            var temp = emissionStateMatrix(roadID)
            temp += (customid + trajid + timest -> prob)
            emissionStateMatrix += (roadID -> temp)
          } else {
            emissionStateMatrix += roadID -> Map(customid + trajid + timest -> prob)
          }
          if (!initialStateDistribution.contains(roadID)) initialStateDistribution += (roadID -> 0)
        })
        stateMatrix += numberOfPointTrajCounter -> roadListForObservation
        numberOfPointTrajCounter += 1
      })
      //here put the initial state probability
      val k = 1.0f / 10
      initialStateDistribution = initialStateDistribution.map(m => (m._1, k.toDouble))

      val beta_parameter: Double = PAR_BETA //𝛽 = 1/ln(2) mediant 􀀲􀀟‖𝑧𝑡 − 𝑧𝑡+1‖𝑔𝑟𝑒𝑎𝑡 𝑐𝑖𝑟𝑐𝑙𝑒− 􀀌𝑥𝑡,𝑖∗ − 𝑥𝑡+1,𝑗 ∗􀀌𝑟𝑜𝑢𝑡𝑒 􀀯􀀳
      var newStateProbMatrix: Map[(Int, String), Map[String, Double]] = Map()
      //calculate foreach road combination road point x - road point x+1 the state transition probability updated with new path lengt
      for (i <- 0 to obsMap.length - 2) {
        val pointDist = DistanceCalculatorImpl().calculateDistanceInMeter(Location(obsMap(i)._1, obsMap(i)._2), Location(obsMap(i + 1)._1, obsMap(i + 1)._2))
        stateMatrix(i).foreach(segment => {
          var valueContainerTemp: Map[String, Double] = Map()
          stateMatrix(i + 1).foreach(segmentSucc => {
            if (adjacencyMap.contains((segment, segmentSucc))) {
              //val listPath = bstateProbabilitiesMatrix.value(segment)(segmentSucc)._2
              val listPath = adjacencyMap((segment, segmentSucc))
              val segLenghtSum = newSegPathLenght(listPath, (obsMap(i)._1, obsMap(i)._2), (obsMap(i + 1)._1, obsMap(i + 1)._2))
              //here calculate the probability
              val stateTransProb = (1 / beta_parameter) * Math.pow(Math.E, (-Math.abs(pointDist - segLenghtSum)) / beta_parameter)
              if (stateTransProb < 0) valueContainerTemp += segmentSucc -> 0
              else valueContainerTemp += segmentSucc -> stateTransProb
            } else valueContainerTemp += segmentSucc -> 0
          })
          newStateProbMatrix += (i, segment) -> valueContainerTemp
        })
      }

      //generate HMM model and run Viterbi of it
      val model = new HmmModel(
        newStateProbMatrix,
        emissionStateMatrix,
        initialStateDistribution,
        initialStateDistribution.toList.length)
      val result = AlgoViterbi.run(model, observations, stateMatrix)

      var realResult = ListBuffer[(Int, String)]()
      if (!result.isEmpty) {
        //recostruction of the route
        for (i <- result.indices) {
          if (result(i) != null) {
            if (i == result.length - 1) realResult = realResult += ((obsNum(i), result(i)))
            else if (!result(i).equals(result(i + 1)) && adjacencyMap.contains(result(i), result(i + 1))) {
               val tmp = adjacencyMap(result(i), result(i + 1)).unzip._1.dropRight(1)
              val newRes = tmp.map(x => (obsNum(i), x))
              realResult = realResult ++ newRes
            }
          }
        }
      }

      //outputted rdd is composed by customid - trajid - path mapmatched
      //println(customid + " " + trajid + " " + realResult)
      //explode path to save inside a table or in a file
      realResult.map(y => (customid, trajid, y._1, y._2))
    }).cache()

    //save result in HIVE TABLE
    val resultWithColumnNames: DataFrame = resutlViterbis.toDF("customid", "trajid", "timest", "roadID")
    resultWithColumnNames.write.mode(SaveMode.Append).saveAsTable(outputTableName)

    /*
    //save result in csv file
    resultContainer.write.format("com.databricks.spark.csv").save("/output")
    */
  }}
