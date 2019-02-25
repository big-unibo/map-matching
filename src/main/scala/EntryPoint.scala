import org.apache.log4j.{Level, Logger}
import org.apache.spark.serializer.KryoSerializer
import org.apache.spark.sql._
import org.datasyslab.geosparksql.utils.{GeoSparkSQLRegistrator}
import org.apache.spark.sql.functions.udf
import org.apache.spark.sql.functions._
import org.datasyslab.geospark.serde.GeoSparkKryoRegistrator
import org.osgeo.proj4j.{CRSFactory, CoordinateTransformFactory, ProjCoordinate}
import scala.collection.mutable.ListBuffer
import org.apache.spark.sql.expressions.Window

object EntryPoint extends App {
  {

    /*
  //COMANDI PARTENZA MASSIMA CAPACITà
  spark2-submit \
      --driver-java-options "-Dhive.metastore.uris=thrift://isi-bigcluster1.csr.unibo.it:9083" \
      --files=/etc/hive/conf/hive-site.xml \
      --num-executors 20 \
      --executor-memory 10G \
      --executor-cores 3 \
      --conf="spark.driver.memory=15G" \
      --master yarn \
      --deploy-mode cluster \
      --class "EntryPoint" \
      /home/egallinucci/mapmatching/progettoTesi-assembly-0.1.jar \
      alpha=4 \
      beta=10 \
      tau=100 \
      theta=4 \
      gamma=200 \
      top50

    //COMANDI PARTENZA IN LOCALE
    spark2-submit \
        --driver-java-options "-Dhive.metastore.uris=thrift://isi-bigcluster1.csr.unibo.it:9083" \
        --files=/etc/hive/conf/hive-site.xml \
        --class "EntryPoint" \
        /home/fvitali/prova.jar

    //COMANDI PARTENZA SHELL + COMANDI DI BASE PER INIZIARE
    spark2-shell \
        --driver-java-options "-Dhive.metastore.uris=thrift://isi-bigcluster1.csr.unibo.it:9083" \
        --files=/etc/hive/conf/hive-site.xml \
        --packages graphframes:graphframes:0.6.0-spark2.2-s_2.11 \
        --packages org.datasyslab:geospark:1.1.3,org.datasyslab:geospark-sql_2.2:1.1.3,org.datasyslab:geospark-viz:1.1.3 \
        --conf spark.serializer=org.apache.spark.serializer.KryoSerializer \
        --conf spark.kryo.registrator=org.datasyslab.geosparkviz.core.Serde.GeoSparkVizKryoRegistrator

    import org.apache.spark.sql.SparkSession
    import scala.collection.mutable.ListBuffer
    val spark = SparkSession.builder.enableHiveSupport.getOrCreate
    val sc = spark.sparkContext
    spark.sql("use trajectory")
  */

    //TEST ALGORITMO DI VITERBI
    /*val obs = List("normal", "cold", "dizzy")
  val states = List("Healthy", "Fever")
  val start_p = Map("Healthy"->0.6,"Fever"->0.4)
  val trans_p = Map("Healthy"->Map("Healthy"->0.7, "Fever"->0.3),"Fever"->Map("Healthy"->0.4, "Fever"->0.6))
  val emit_p = Map("Healthy"->Map("normal"->0.5, "cold"->0.4, "dizzy"->0.1),"Fever"->Map("normal"->0.1, "cold"->0.3, "dizzy"->0.6))
  val model = new HmmModel(trans_p,emit_p,start_p,states.length)
  val result = AlgoViterbi.run(model,obs,states)
  result.foreach(x=>print(x))*/

    Logger.getLogger("org").setLevel(Level.WARN)
    Logger.getLogger("akka").setLevel(Level.WARN)

    val numExecutor = 10
    val numCores = 6
    val numRepartition = numExecutor*numCores*3

    if (args.size < 5) {
      throw new IllegalArgumentException("Not enough parameters. Example usage: alpha=4 beta=10 tau=100 theta=4 gamma=200")
    }

    val PAR_ALPHA = args(0).split("=")(1).toInt
    val PAR_BETA = args(1).split("=")(1).toInt
    val PAR_TAU = args(2).split("=")(1).toInt
    val PAR_THETA = args(3).split("=")(1).toInt
    val PAR_GAMMA = args(4).split("=")(1)
    val PAR_TOP50 = args.size == 6

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
    spark.sql("use trajectory")
    val tablename = "MapMatchedTrajectories"
    spark.sql("drop table if exists " + tablename)

    //PRENDO LE TABELLE DI OSM DA HIVE CHE MI SERVONO
    val osmWaysDF = spark.sql("select * from osmwaysmilan").withColumnRenamed("id", "wayID").withColumnRenamed("tags", "featureTags").withColumnRenamed("nodes", "nodesList")
    val osmNodesDF = spark.sql("select * from osmnodesmilan")

    //FILTRAGGIO SOLO STRADE "VERE" DATI GLI ATTRIBUTI
    val onlyHighwayThatRepresentRoads = List("motorway", "trunk", "primary", "secondary", "tertiary", "unclassified", "residential")
    val onlyRoadsDF = osmWaysDF.where(col("featureTags")("highway").isNotNull).filter(col("featureTags")("highway").isin(onlyHighwayThatRepresentRoads: _*)) //prendo solo le strade vere

    //ESPLODO LA COLONNA DELLA LISTA DI NODI DI CUI SONO COMPOSTE LE STRADE
    val explodeNodes = onlyRoadsDF.select(onlyRoadsDF("*"), posexplode(onlyRoadsDF("nodesList")))

    //SOSTITUZIONE LISTA NODI CON LISTA COORDINATE GPS -> JOIN SUI NODI PER PRENDERE LE REALI COORDINATE, CALCOLO LUNGHEZZA STRADA E RI-RAGGRUPPAMENTO
    val joinNodesToTakeCordsDF = explodeNodes.join(osmNodesDF, explodeNodes("col") === osmNodesDF("id")).select(explodeNodes("wayID"), explodeNodes("timestamp"), explodeNodes("version"), explodeNodes("changesetid"), explodeNodes("featureTags"), osmNodesDF("latitude"), osmNodesDF("longitude"), explodeNodes("pos").as("pointPosition"))
    val concatenateCoordinates = udf((first: String, second: String) => Seq(first.toDouble, second.toDouble))
    val unifiedCordsDF = joinNodesToTakeCordsDF.select(joinNodesToTakeCordsDF("wayID"), joinNodesToTakeCordsDF("timestamp"), joinNodesToTakeCordsDF("version"), joinNodesToTakeCordsDF("changesetid"), joinNodesToTakeCordsDF("featureTags"), joinNodesToTakeCordsDF("latitude"), joinNodesToTakeCordsDF("longitude"), joinNodesToTakeCordsDF("pointPosition")).withColumn("coordinates", concatenateCoordinates(joinNodesToTakeCordsDF("longitude"), joinNodesToTakeCordsDF("latitude"))).orderBy(col("wayID"), col("pointPosition"))
    val regroupWayCordsDF = unifiedCordsDF.groupBy("wayID", "timestamp", "version", "changesetid").agg(collect_list("coordinates").as("coordinates")).withColumnRenamed("wayID", "roadID")
    val calcLenght = udf((coordList: Seq[Seq[Double]]) => DistanceCalculatorImpl().calculateDistanceInMeter(Location(coordList.head.last, coordList.head.head), Location(coordList.last.last, coordList.last.head)))
    val roadDF = regroupWayCordsDF.join(onlyRoadsDF, regroupWayCordsDF("roadID") === onlyRoadsDF("wayID")).withColumn("lenght", calcLenght(col("coordinates"))).select(regroupWayCordsDF("*"), onlyRoadsDF("featureTags"), col("lenght"), onlyRoadsDF("nodesList"))
    val roadFirstAndLastPoint = regroupWayCordsDF.withColumn("initialPoint", col("coordinates").getItem(0)).withColumn("lastPoint", col("coordinates").apply(size(col("coordinates")).minus(1))).select(col("roadID"), col("initialPoint"), col("lastPoint"), col("coordinates"))

    //CREAZIONE MAPPA PER MANTENERE PER OGNI SEGMENTO, IL PUNTO DI INIZIO E DI FINE PER IL CALCOLO DELLA PROIEZIONE ORTOGONALE DURANTE VITERBI
    var segInFinMap: Map[String, ((Double, Double), (Double, Double))] = roadFirstAndLastPoint.collect().map(r => {
      val initialPoint: Seq[Double] = r.getAs[Seq[Double]]("initialPoint")
      val lastPoint: Seq[Double] = r.getAs[Seq[Double]]("lastPoint")
      r.getAs[String]("roadID") -> ((initialPoint.last, initialPoint.head), (lastPoint.last, lastPoint.head))
    }).toMap
    //BROADCASTO MAPPA, CIRCA 25MB
    var bsegInFinMap = spark.sparkContext.broadcast(segInFinMap)

    //CREAZIONE TABELLA STRADE VICINE DATI NODI IN COMUNE NELLA LISTA DEI NODI
    val explodeNodesAfterLenghtCalc = roadDF.withColumn("nodes", explode(onlyRoadsDF("nodesList"))).select(col("roadID"), col("nodes"), col("lenght"))
    val firstJoin = explodeNodesAfterLenghtCalc.as("waySottoOsservazione").join(explodeNodesAfterLenghtCalc.as("wayAttigua"), col("waySottoOsservazione.nodes") === col("wayAttigua.nodes") && col("waySottoOsservazione.roadID") =!= col("wayAttigua.roadID")).select(col("waySottoOsservazione.roadID"), col("waySottoOsservazione.lenght"), col("wayAttigua.roadID").as("roadIDAttigua"), col("wayAttigua.lenght").as("wayAttiguaLenght"))
    //firstJoin.cache()
    firstJoin.createOrReplaceTempView("firstJoin")
    val dfDist = spark.sql("SELECT * FROM firstJoin DISTRIBUTE BY roadID,roadIDAttigua").repartition(col("roadID"),col("roadIDAttigua"))
    dfDist.createOrReplaceTempView("completeNeighborhood")
    spark.sql("CACHE TABLE completeNeighborhood")
    //firstJoin.createOrReplaceTempView("completeNeighborhood")
    //firstJoin.count()
    val groupWay = firstJoin.groupBy(col("roadID"), col("lenght")).agg(collect_list(map(col("roadIDAttigua"), col("wayAttiguaLenght"))).as("stradeAttigue"))

    /**
      * metodo per calcolare la query di join per calcolo path vicinato
      *
      * @param neighborhoodTable la tabella che comprende i segmenti tra le strade candidate di tutte le traietotrie sotto esame
      * @param tableName         tabella del vicinato a distanza 1 completa
      * @param deep              profondita di ricerca dei path
      * @return la query in formato stringa
      */
    def createOmegaJoin(neighborhoodTable: String, tableName: String, deep: Int): String = {
      val chars = 'a' to 'z'
      var selectString = "SELECT DISTINCT "
      var fromString = "FROM "
      var whereString = "WHERE "
      val listChars = neighborhoodTable :: chars.toList
      for (i <- 0 until deep) {
        if (i == deep - 1) {
          selectString = selectString.concat(listChars(i) + ".roadID AS " + "roadID" + listChars(i) + ", " + listChars(i) + ".lenght AS " + "lenght" + listChars(i) + " ")
        } else selectString = selectString.concat(listChars(i) + ".roadID AS " + "roadID" + listChars(i) + ", " + listChars(i) + ".lenght AS " + "lenght" + listChars(i) + ", ")
        if (i == 0) fromString = fromString.concat(listChars(i) + " ,")
        else if (i == deep - 1) {
          fromString = fromString.concat(tableName + " " + listChars(i) + " ")
        } else fromString = fromString.concat(tableName + " " + listChars(i) + " ,")
        if (i != 0 && i != deep - 1) {
          val tempSubCondition = listChars.take(i).map(_ + ".roadID != " + listChars(i) + ".roadID ").mkString(" AND ")
          whereString = whereString.concat(listChars(i - 1) + ".roadIDAttigua = " + listChars(i) + ".roadID AND " + tempSubCondition + " AND ")
        }
        else if (i == deep - 1) {
          val tempSubCondition = listChars.take(i).map(_ + ".roadID != " + listChars(i) + ".roadID ").mkString(" AND ")
          whereString = whereString.concat(listChars(i - 1) + ".roadIDAttigua = " + listChars(i) + ".roadID AND " + tempSubCondition)
        }
      }
      selectString.concat(fromString.concat(whereString))
    }

    /**
      * metodo per calcolare la query di join per calcolo path vicinato
      *
      * @param neighborhoodTable la tabella che comprende i segmenti tra le strade candidate di tutte le traietotrie sotto esame
      * @param tableName         tabella del vicinato a distanza 1 completa
      * @param deep              profondita di ricerca dei path
      * @param theresold         soglia massima lunghezza path
      * @return la query in formato stringa
      */
    def createOmegaLeftJoin(neighborhoodTable: String, tableName: String, deep: Int, theresold:String): String = {
      val chars = 'a' to 'z'
      var selectString = "SELECT "
      val fromString = "FROM "+neighborhoodTable+" "
      var leftOuterString =""
      //var whereString = "WHERE "
      val listChars = neighborhoodTable :: chars.toList
      for (i <- 0 until deep) {
        if (i == deep - 1) {
          selectString = selectString.concat(listChars(i) + ".roadID AS " + "roadID" + listChars(i) + ", " + listChars(i) + ".lenght AS " + "lenght" + listChars(i) +", "+listChars(i) + ".roadIDAttigua AS " + "roadIDAttigua" + listChars(i) + ", " + listChars(i) + ".wayAttiguaLenght AS " + "wayAttiguaLenght" + listChars(i) + " ")
        }
        else selectString = selectString.concat(listChars(i) + ".roadID AS " + "roadID" + listChars(i) + ", " + listChars(i) + ".lenght AS " + "lenght" + listChars(i) + ", ")
        if (i== deep-1){
          val tempSubCondition = listChars.take(i).map(_ + ".roadID != " + listChars(i) + ".roadID ").mkString(" AND ")
          val sumCondition = listChars.take(i).map(_+".lenght").mkString("+")
          val additionalEndCondition = listChars.take(i).map(_ + ".roadID != " + listChars(i) + ".roadIDAttigua ").mkString(" AND ")
          leftOuterString = leftOuterString.concat("LEFT OUTER JOIN " + tableName + " " + listChars(i) + " ON " + listChars(i - 1) + ".roadIDAttigua = " + listChars(i) + ".roadID AND " + tempSubCondition + " AND " + additionalEndCondition +" AND ("+sumCondition+") < "+theresold+" ")
        }
        else if (i != 0 && i< deep-1){
          val tempSubCondition = listChars.take(i).map(_ + ".roadID != " + listChars(i) + ".roadID ").mkString(" AND ")
          val sumCondition = listChars.take(i).map(_+".lenght").mkString("+")
          leftOuterString = leftOuterString.concat("LEFT OUTER JOIN "+tableName+" "+listChars(i)+ " ON "+listChars(i-1)+".roadIDAttigua = "+listChars(i)+".roadID AND "+tempSubCondition +" AND ("+sumCondition+") < "+theresold+" ")
        }

      }
      selectString.concat(fromString.concat(leftOuterString))
    }

    def createOmegaGeosparkJoin(neighborhoodTable: String, tableName: String, deep: Int): String = {
      val chars = 'a' to 'z'
      var selectString = "SELECT DISTINCT "
      var fromString = "FROM "
      var whereString = "WHERE "
      val listChars = neighborhoodTable :: chars.toList
      for (i <- 0 until deep) {
        if (i == deep - 1) {
          selectString = selectString.concat(listChars(i) + ".roadID AS " + "roadID" + listChars(i) + ", " + listChars(i) + ".lenght AS " + "lenght" + listChars(i) + " ")
        } else selectString = selectString.concat(listChars(i) + ".roadID AS " + "roadID" + listChars(i) + ", " + listChars(i) + ".lenght AS " + "lenght" + listChars(i) + ", ")
        if (i == 0) fromString = fromString.concat(listChars(i) + " ,")
        else if (i == deep - 1) {
          fromString = fromString.concat(tableName + " " + listChars(i) + " ")
        } else fromString = fromString.concat(tableName + " " + listChars(i) + " ,")
        if (i != 0 && i != deep - 1) {
          val tempSubCondition = listChars.take(i).map(_ + ".roadID != " + listChars(i) + ".roadID ").mkString(" AND ")
          whereString = whereString.concat("ST_Intersects(" + listChars(i-1) + ".geoElement,"+ listChars(i) + ".geoElement)"+" AND " + tempSubCondition + " AND ")
        }
        else if (i == deep - 1) {
          val tempSubCondition = listChars.take(i).map(_ + ".roadID != " + listChars(i) + ".roadID ").mkString(" AND ")
          whereString = whereString.concat("ST_Intersects(" + listChars(i-1) + ".geoElement,"+ listChars(i) + ".geoElement)"+" AND " + tempSubCondition)
        }
      }
      selectString.concat(fromString.concat(whereString))
    }


    //val prova = firstJoin.limit(50)
    //prova.createOrReplaceTempView("neigh")
    //var dededee = spark.sql(createOmegaJoin("neigh","prova",4))
    //val secondJoin = firstJoin.as("first").join(firstJoin.as("second"),col("first.roadIDAttigua") === col("second.roadID")).join(firstJoin.as("second"),col("first.roadIDAttigua") === col("second.roadID")).select(col("first.*"),col("second.roadID"),col("")).join(firstJoin.as("third"),col("second.roadIDAttigua") === col("third.roadID"))
    //val terzoJoin = secondJoin.as("first").join(secondJoin.as("second"),col("first.roadIDAttigua") === col("second.roadID"))
    //GIRO OBBLIGATO CON CREAZIONE STRUTTURA A MANO PER ESSERE POI CONVERTITO IN SPATIAL RDD
    val roadDFnew = roadDF.withColumnRenamed("roadID", "firstWay").withColumnRenamed("lenght","roadLenght")
    val joinOnlyRoad = roadDFnew.join(groupWay, roadDFnew("firstWay") === groupWay("roadID"))
    val monoStructCreateCorrectStruct = joinOnlyRoad.select(struct(lit("Feature").as("type"), col("roadID").as("id"), struct(col("timestamp"), col("version"), col("changesetid").as("changeset"),col("lenght"), col("featureTags.highway").as("highway"), col("featureTags.loc_ref").as("loc_ref"), col("featureTags.name").as("name"), col("featureTags.oneway").as("oneway"), col("featureTags.surface").as("surface")).alias("properties"), struct(lit("LineString").as("type"), col("coordinates")).alias("geometry")).alias("toJson")).withColumn("json", to_json(col("toJson")))
    monoStructCreateCorrectStruct.createOrReplaceTempView("roadDF")

    /*
    //PRENDO SOLO MAPPA STRADA -> STRADE VICINE
    var stateTransitionProbabilities: Map[(String, Double), Seq[Map[String, Double]]] = groupWay.collect().map(r => {
      (r.getAs[String]("roadID"), r.getAs[Double]("lenght")) -> r.getAs[Seq[Map[String, Double]]]("stradeAttigue")
    }).toMap
    */


    // Vertex DataFrame
    /*val v = spark.createDataFrame(List(
      ("a", "Alice", 34),
      ("b", "Bob", 36),
      ("c", "Charlie", 30),
      ("d", "David", 29),
      ("e", "Esther", 32),
      ("f", "Fanny", 36),
      ("g", "Gabby", 60)
    )).toDF("id", "name", "age")
    // Edge DataFrame
    val e = spark.createDataFrame(List(
      ("a", "b", "friend"),
      ("b", "c", "follow"),
      ("c", "b", "follow"),
      ("f", "c", "follow"),
      ("e", "f", "follow"),
      ("e", "d", "friend"),
      ("d", "a", "friend"),
      ("a", "e", "friend")
    )).toDF("src", "dst", "relationship")
    val g = GraphFrame(v, e)
    //val filteredPaths = g.bfs.fromExpr("name != 'swsww'").toExpr("age>0").maxPathLength(3).run()
    val filteredPaths = g.bfs.fromExpr("name = 'Esther'").toExpr("age < 32").edgeFilter("relationship != 'friend'").maxPathLength(3).run()
    */

    /*

    PROVA CON GRAPHFRAMES, CALCOLO DEL VICINATO
    val vertex = roadDF.select("roadID","lenght").withColumnRenamed("roadID","id")
    val edges = firstJoin.select("roadID","roadIDAttigua").withColumnRenamed("roadID","src").withColumnRenamed("roadIDAttigua","dst")
    val roadGraph = GraphFrame(vertex, edges)
    //val chain4 = roadGraph.find("(a)-[]->(b); (b)-[]->(c); (c)-[]->(d); (d)-[]->(e); (e)-[]->(f)").filter("c!=a AND d!=b AND d!=a AND e!=c AND e!=b AND e!=a AND f!=d AND f!=c AND f!=b AND f!=a")

    def createFindAndFilterPattern(deep: Int,isFind:Boolean): String = {
      val chars = 'a' to 'z'
      val listChars = chars.toList
      var output = ""
      for (i <- 1 until deep) {
        if(isFind){
          if(i!=deep-1) output=output.concat("("+listChars(i-1)+")-[]->("+listChars(i)+"); ")
          else output= output.concat("("+listChars(i-1)+")-[]->("+listChars(i)+") ")
        }
        else{
          if(i>1){
            val tempSubCondition = listChars.take(i-1).map(listChars(i) + " != " + _).mkString(" AND ")
            if(i!=deep-1)output= output.concat(tempSubCondition+" AND ")
            else output.concat(tempSubCondition)
          }
        }
      }
      output
    }
    val deep = 10
    val allPath = roadGraph.find(createFindAndFilterPattern(deep,isFind = true)).filter(createFindAndFilterPattern(6,isFind = false))
    //allPath.show()

    val allPossibilities = allPath.rdd.map(row=>{
      var listPoss:List[List[(String,Double)]] = List()
      var temp:List[(String,Double)] = List()
      for(i <- 0 until deep) {
        val rowTmp = row.getAs[Row](i)
        temp = temp :+ (rowTmp.getAs[String]("id"),rowTmp.getAs[Double]("lenght"))
        listPoss = listPoss :+ temp
      }
      listPoss
    })

    val flatAllPos = allPossibilities.flatMap(list=>list).groupBy(x=>(x.head._1,x.last._1)).mapValues(_.minBy(_.unzip._2.sum))

    val adjacencyMap:Map[String,Map[String,List[(String,Double)]]] = flatAllPos.collect().map(r=> {
      r._1._1 -> Map(r._1._2->r._2)
    }).toMap

    //adjacencyMap.foreach(x=>println(x))
*/

    /*
    //VARIE PROVE CON GRAPHX, FALLIMENTARE
      def dijkstra[VD](g:Graph[VD,Double], origin:VertexId) = {
        var g2 = g.mapVertices(
          (vid, vd) => (false, if (vid == origin) 0 else Double.MaxValue,
            List[VertexId]()))
        for (i <- 1L to g.vertices.count - 1) {
          val currentVertexId =
            g2.vertices.filter(!_._2._1)
              .fold((0L, (false, Double.MaxValue, List[VertexId]())))((a, b) =>
                if (a._2._2 < b._2._2) a else b)
              ._1
          val newDistances = g2.aggregateMessages[(Double, List[VertexId])](
            ctx => if (ctx.srcId == currentVertexId)
              ctx.sendToDst((ctx.srcAttr._2 + ctx.attr,
                ctx.srcAttr._3 :+ ctx.srcId)),
            (a, b) => if (a._1 < b._1) a else b)
          g2 = g2.outerJoinVertices(newDistances)((vid, vd, newSum) => {
            val newSumVal =
              newSum.getOrElse((Double.MaxValue, List[VertexId]()))
            (vd._1 || vid == currentVertexId,
              math.min(vd._2, newSumVal._1),
              if (vd._2 < newSumVal._1) vd._3 else newSumVal._2)
          })
        }
        g.outerJoinVertices(g2.vertices)((vid, vd, dist) =>
          (vd, dist.getOrElse((false, Double.MaxValue, List[VertexId]()))
            .productIterator.toList.tail))
      }


      val streetVertices: RDD[(VertexId,( String,Double))] = roadDF.rdd.map(x => (MurmurHash3.stringHash(x.getAs[String]("roadID")), (x.getAs[String]("roadID"),x.getAs [Double]("lenght"))))
      val streetEdges: RDD[Edge[Double]]= firstJoin.rdd.map(x => Edge(MurmurHash3.stringHash(x.getAs[String]("roadID")),MurmurHash3.stringHash(x.getAs[String]("roadIDAttigua")),x.getAs[Double]("lenght")))
      val streetGraph = Graph(streetVertices, streetEdges)
      //val result = streetVertices.flatMap(x=> shortestPath(streetGraph,x._1))
      //val resulteeee = shortestPath(streetGraph, MurmurHash3.stringHash("W264646379"))
      //dijkstra(streetGraph, MurmurHash3.stringHash("W264646379")).vertices.map(_._2).collect
      //val result = ShortestPaths.run(streetGraph, Seq( MurmurHash3.stringHash("W264646379")))
      var idSegId = streetVertices.collectAsMap()
      val resultone = idSegId.map(x=>shortestPath(streetGraph,x._1)).toList.flatten.toDF("idInizio","idFine","listPath")
      resultone.show(500)

      val myVertices = spark.sparkContext.makeRDD(Array((1L, "A"), (2L, "B"), (3L, "C"),
      (4L, "D"), (5L, "E"), (6L, "F"), (7L, "G")))
      val myEdges = spark.sparkContext.makeRDD(Array(Edge(1L, 2L, 7.0), Edge(1L, 4L, 5.0),
      Edge(2L, 3L, 8.0), Edge(2L, 4L, 9.0), Edge(2L, 5L, 7.0),
      Edge(3L, 5L, 5.0), Edge(4L, 5L, 15.0), Edge(4L, 6L, 6.0),
      Edge(5L, 6L, 8.0), Edge(5L, 7L, 9.0), Edge(6L, 7L, 11.0)))
      val myGraph = Graph(myVertices, myEdges)
      //dijkstra(myGraph, 1L).vertices.map(_._2).collect
      //val result2 = shortestPath(myGraph,1L)
      var idSegId2 = myVertices.collectAsMap()
      val resultone2 = idSegId2.map(x=>shortestPath(myGraph,x._1)).toList.flatten.toDF("idInizio","idFine","listPath")
    */

    /*
    //MAPPA PRECOMPUTATA DELLE ADIACENZE FORMATA DA SEGMENTO -> SEGMENTO (LISTA DI SEGMENTI PER ARRIVARCI)
    def createStateProbabilitiesMatrix(stateTransitionProb: Map[(String, Double), Seq[Map[String, Double]]]): Map[String, Map[String, (Int, List[(String, Double)])]] = {
      var stateProbabilitiesMatrix: Map[String, Map[String, (Int, List[(String, Double)])]] = Map()
      stateTransitionProb.foreach(segment => {
        var mapStateVal: Map[String, (Int, List[(String, Double)])] = Map()
        val segLenght = segment._1._2
        val segList = List[(String, Double)](segment._1)
        segment._2.foreach(segmentVicino => {
          val segLenghtPhase2 = segLenght + segmentVicino.head._2
          val segListPhase2 = segList :+ segmentVicino.head
          stateTransitionProb(segmentVicino.head).foreach(segmentAdUnSegmento => {
            val segLenghtPhase3 = segLenghtPhase2 + segmentVicino.head._2
            val segListPhase3 = segListPhase2 :+ segmentAdUnSegmento.head
            if (segLenghtPhase3 < 800) {
              stateTransitionProb(segmentAdUnSegmento.head).foreach(segmentAdDueSegmenti => {
                val segLenghtPhase4 = segLenghtPhase3 + segmentAdDueSegmenti.head._2
                val segListPhase4 = segListPhase3 :+ segmentAdDueSegmenti.head
                if (segLenghtPhase4 < 800) {
                  stateTransitionProb(segmentAdDueSegmenti.head).foreach(segmentAdTreSegmenti => {
                    val segLenghtPhase5 = segLenghtPhase4 + segmentAdTreSegmenti.head._2
                    val segListPhase5 = segListPhase4 :+ segmentAdTreSegmenti.head
                    if (segLenghtPhase5 < 800) {
                      stateTransitionProb(segmentAdTreSegmenti.head).foreach(segmentAQuadSegmenti => {
                        val segLenghtPhase6 = segLenghtPhase5 + segmentAQuadSegmenti.head._2
                        val segListPhase6 = segListPhase5 :+ segmentAQuadSegmenti.head
                        if (segLenghtPhase6 < 800) {
                          stateTransitionProb(segmentAQuadSegmenti.head).foreach(segmentAFifSegmenti => {
                            val segLenghtPhase7 = segLenghtPhase6 + segmentAFifSegmenti.head._2
                            val segListPhase7 = segListPhase6 :+ segmentAFifSegmenti.head
                            if (!mapStateVal.contains(segmentAFifSegmenti.head._1)) {
                              mapStateVal += (segmentAFifSegmenti.head._1 -> (6, segListPhase7))
                            }
                            /*if (segLenghtPhase7 < 800) {
                            stateTransitionProb(segmentAFifSegmenti.head).foreach(segmentASixSegmenti => {
                              val segLenghtPhase8 = segLenghtPhase7 + segmentASixSegmenti.head._2
                              val segListPhase8 = segListPhase7 :+ segmentASixSegmenti.head
                              if (segLenghtPhase8 < 800) {
                                stateTransitionProb(segmentASixSegmenti.head).foreach(segmentASevenSegmenti => {
                                  val segLenghtPhase9 = segLenghtPhase8 + segmentASevenSegmenti.head._2
                                  val segListPhase9 = segListPhase8 :+ segmentASevenSegmenti.head
                                  if (!mapStateVal.contains(segmentASevenSegmenti.head._1)) {
                                    mapStateVal += (segmentASevenSegmenti.head._1 -> (8, segListPhase9))
                                  }
                                })
                              }
                              if (mapStateVal.contains(segmentASixSegmenti.head._1)) {
                                if (mapStateVal(segmentASixSegmenti.head._1)._1 > 7) {
                                  mapStateVal += (segmentASixSegmenti.head._1 -> (7, segListPhase8))
                                }
                              } else {
                                mapStateVal += (segmentASixSegmenti.head._1 -> (7, segListPhase8))
                              }
                            })
                          }
                          if (mapStateVal.contains(segmentAFifSegmenti.head._1)) {
                            if (mapStateVal(segmentAFifSegmenti.head._1)._1 > 6) {
                              mapStateVal += (segmentAFifSegmenti.head._1 -> (6, segListPhase7))
                            }
                          } else {
                            mapStateVal += (segmentAFifSegmenti.head._1 -> (6, segListPhase7))
                          }*/
                          })
                        }
                        if (mapStateVal.contains(segmentAQuadSegmenti.head._1)) {
                          if (mapStateVal(segmentAQuadSegmenti.head._1)._1 > 5) {
                            mapStateVal += (segmentAQuadSegmenti.head._1 -> (5, segListPhase6))
                          }
                        } else {
                          mapStateVal += (segmentAQuadSegmenti.head._1 -> (5, segListPhase6))
                        }
                      })
                    }
                    if (mapStateVal.contains(segmentAdTreSegmenti.head._1)) {
                      if (mapStateVal(segmentAdTreSegmenti.head._1)._1 > 4) {
                        mapStateVal += (segmentAdTreSegmenti.head._1 -> (4, segListPhase5))
                      }
                    } else {
                      mapStateVal += (segmentAdTreSegmenti.head._1 -> (4, segListPhase5))
                    }
                  })
                }
                if (mapStateVal.contains(segmentAdDueSegmenti.head._1)) {
                  if (mapStateVal(segmentAdDueSegmenti.head._1)._1 > 3) {
                    mapStateVal += (segmentAdDueSegmenti.head._1 -> (3, segListPhase4))
                  }
                } else {
                  mapStateVal += (segmentAdDueSegmenti.head._1 -> (3, segListPhase4))
                }
              })
            }
            if (mapStateVal.contains(segmentAdUnSegmento.head._1)) {
              if (mapStateVal(segmentAdUnSegmento.head._1)._1 > 2) {
                mapStateVal += (segmentAdUnSegmento.head._1 -> (2, segListPhase3))
              }
            } else {
              mapStateVal += (segmentAdUnSegmento.head._1 -> (2, segListPhase3))
            }
          })
          if (mapStateVal.contains(segmentVicino.head._1)) {
            if (mapStateVal(segmentVicino.head._1)._1 > 1) {
              mapStateVal += (segmentVicino.head._1 -> (1, segListPhase2))
            }
          } else {
            mapStateVal += (segmentVicino.head._1 -> (1, segListPhase2))
          }
        })
        mapStateVal += segment._1._1 -> (0, List(segment._1))
        stateProbabilitiesMatrix += segment._1._1 -> mapStateVal
      })
      stateProbabilitiesMatrix
    }

    val stateProbabilitiesMatrix: Map[String, Map[String, (Int, List[(String, Double)])]] = createStateProbabilitiesMatrix(stateTransitionProbabilities)
    //BROADCAST DELLA MAPPA DELLE ADIACENZE, CIRCA 1GB
    var bstateProbabilitiesMatrix = spark.sparkContext.broadcast(stateProbabilitiesMatrix)
*/

      //VECCHIA IMPLEMENTAZIONE PAPER STUPIDO
      /*var stateProbabilitiesMatrix:Map[String,Map[String,Double]] = Map()
      finalStateProb.foreach(x=>{
        var mapStateVal:Map[String,Double] = Map()
        x._2.foreach(y=>{
          y._2.foreach(z=>{
            if(!(mapStateVal.exists(_ == (z->0.45)) || mapStateVal.exists(_ == (z->0.6)))){
              mapStateVal += z -> 0.15
            }
          })
          mapStateVal += y._1 -> 0.45
        })
        mapStateVal += x._1 -> 0.6
        stateProbabilitiesMatrix += x._1 -> mapStateVal
      })*/



      //CREAZIONE COLONNA GEOMETRICA SECONDO DIRETTIVE GEOSPARK
      var geoRoadDF = spark.sql(
        """
          |SELECT ST_GeomFromGeoJSON(json) AS geoElement, toJson.id AS roadID, COALESCE(toJson.properties.name , "noName") AS roadName, toJson.properties.lenght AS lenght
          |FROM roadDF
        """.stripMargin)
      geoRoadDF.createOrReplaceTempView("geoRoadDF")

      //CONVERSIONE COORDINATE IN METRI BASED
      var convGeoRoadDF = spark.sql(
        """
          |SELECT ST_Transform(geoElement, "epsg:4326", "epsg:3857") AS geoElement, roadID, roadName,lenght
          |FROM geoRoadDF
        """.stripMargin)
      convGeoRoadDF.createOrReplaceTempView("convGeoRoadDF")


      //SELEZIONE TRAIETTORIE
      val trajectoryDF = spark.sql("select * from cariploenewfiltermilanonly")
      //trajectoryDF.cache()
      //trajectoryDF.count()

      //TENGO SOLO LE COLONNE CHE MI INTERESSANO
      val fiterTrajDF = trajectoryDF.select("customid","trajid","timest","latitude","longitude","timediff","spacediff","speed","geojson")
      //val minSpeed = 15
      //FILTRAGGIO TRAIETTORIE SECONDO DIRETTIVE - VELOCITà MAGGIORE DI 0, CALCOLO VELOCITà CORRETTA
      val trajectoryMilanSpeedDF = fiterTrajDF.where(col("speed") > 0)
      //FILTRAGGIO CON DATASET TRAIETTORIE CON SPEED SBAGLIATA, LA RICALCOLO A MANO
      //val withSpeedOk = trajectoryMilanSpeedDF.withColumn("timediff2",col("timediff")/3600).withColumn("speed2",col("spacediff")/col("timediff2"))
      //elimino i punti "fermi"
      //val filterBySpeed = withSpeedOk.filter(col("speed2")>minSpeed)
      trajectoryMilanSpeedDF.createOrReplaceTempView("trajTablePreFilter")

      //CON QUESTA QUERY FILTRO LE TRAIETTORIE PRENDENDO SOLO QUELLE CHE HANNO UNA VELOCITà MEDIA SUPERIORE A 20 KM/H E SONO COMPOSTE ALMENO DA 9 PUNTI
      val trajFilteredDF = spark.sql("select * from(" +
      "select *, max(pointOrder) over(partition by customid,trajid) trajPointCount from (select *, avg(spacediff) over(partition by customid,trajid) spaceMedia, avg(timediff) over(partition by customid,trajid) timeMedia," +
      " avg(speed) over(partition by customid,trajid) speedMedia," +
      "  ROW_NUMBER() OVER (PARTITION BY customid,trajid ORDER BY timest) AS pointOrder from trajTablePreFilter) a "+
      " where spaceMedia>0.01 and speedMedia > 25) b " +
        "where b.trajPointCount >25")
      trajFilteredDF.createOrReplaceTempView("trajFiltered")

      var tabTraj = "trajFiltered";
      if(PAR_TOP50) {
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
            "limit 50") //ENRICO - quante traiettorie? altrimenti usa trajFiltered
        //trajInsideMilanCenter.cache()
        //trajInsideMilanCenter.count()
        //trajInsideMilanCenter.show(false)
        trajInsideMilanCenter.createOrReplaceTempView("top50trajPointInsideMilanCenter")

        val top20TrajComplete = spark.sql(
          "select trajFiltered.* " +
            "from trajFiltered JOIN top50trajPointInsideMilanCenter ON (trajFiltered.customid = top50trajPointInsideMilanCenter.customid AND trajFiltered.trajid = top50trajPointInsideMilanCenter.trajid)")
        val trajToWrite = top20TrajComplete.select("customid", "trajid", "latitude", "longitude", "timest")
        //trajToWrite.write.format("com.databricks.spark.csv").save("/trajectories")
        top20TrajComplete.createOrReplaceTempView("top200trajspacemediaminore")

        tabTraj = "top200trajspacemediaminore"
      }


    /*
      //CONVERTO ANCHE QUI I PUNTI SECONDO LIBRERIA GEOJSON
      var preconv = spark.sql(
        """
          |SELECT ST_GeomFromGeoJSON(geojson) AS geoPoint, latitude,longitude,customid,trajid,timest
          |FROM trajFiltered
        """.stripMargin)
      preconv.createOrReplaceTempView("geoTrajFiltered")
      //E TRASFORMO LE COORDINATE IN METER-BASED
      var convPreConv = spark.sql(
        """
          |SELECT ST_Transform(geoPoint, "epsg:4326", "epsg:3857") AS geoPoint, latitude,longitude,customid,trajid,timest
          |FROM geoTrajFiltered
        """.stripMargin)
      //convPreConv.createOrReplaceTempView("convGeoTrajFiltered")


      val changeCoordinateScale = convPreConv.withColumn("realLatitude",col("latitude")).withColumn("realLongitude",col("longitude")).withColumn("latitude", col("latitude") * 10000).withColumn("longitude", col("longitude") * 10000)
      val roundCoordinates = changeCoordinateScale.withColumn("latitude", changeCoordinateScale.col("latitude").cast("int")).withColumn("longitude", changeCoordinateScale.col("longitude").cast("int"))
      val newCoordinates = roundCoordinates.withColumn("latitude", col("latitude") / 10000).withColumn("longitude", col("longitude") / 10000)
      def toPointGeoJSON = udf((lat:Double,lon:Double)=> {
        "{\"type\":\"Point\",\"coordinates\":[" + lon + "," + lat + "]}"
      })
      val gruppone = newCoordinates.groupBy(col("latitude"),col("longitude")).agg(collect_list(struct("customid","trajid","timest","realLatitude","realLongitude","geoPoint")).as("pointInfo")).withColumn("geojson",toPointGeoJSON(col("latitude"),col("longitude")))
      gruppone.createOrReplaceTempView("trajFiltered")
    */

      /*
      //UTILIZZO QUESTA QUERY PER PREDNERE SOLO LE 200 TOP TRAIETTORIE DATA LA VICINANZA MEDIA TRA PUNTI
      val top20Trajectory = spark.sql(
        """
          |SELECT distinct customid,trajid,spaceMedia,speedMedia,timeMedia FROM trajFiltered
          |ORDER BY timeMedia ASC
          |LIMIT 50
        """.stripMargin)
      top20Trajectory.cache()
      top20Trajectory.count()
      top20Trajectory.show(500,false)
      top20Trajectory.createOrReplaceTempView("top200Traj")*/

      //VADO AD ESTRARRE TUTTI I PUNTI


      /*
      val top20Trajectory = spark.sql(
        """
          |SELECT * FROM trajFiltered
          |WHERE customid="01d2e868ad56c00eb150916364b3cff8036af28c7410c03b9181d07d22b8e976" AND trajid=630
        """.stripMargin)
      top20Trajectory.createOrReplaceTempView("top200trajspacemediaminore")
    */


      //CONVERTO ANCHE QUI I PUNTI SECONDO LIBRERIA GEOJSON
      val explodetrajConverted = spark.sql(
        ("""
          |SELECT ST_GeomFromGeoJSON(geojson) AS geoPoint, latitude,longitude,customid,trajid,timest
          |FROM """+tabTraj+"""
        """).stripMargin)
      explodetrajConverted.createOrReplaceTempView("geoTrajFiltered")
      //E TRASFORMO LE COORDINATE IN METER-BASED
      val convExplodetrajConverted = spark.sql(
        """
          |SELECT ST_Transform(geoPoint, "epsg:4326", "epsg:3857") AS geoPoint, latitude,longitude,customid,trajid,timest
          |FROM geoTrajFiltered
        """.stripMargin)
      convExplodetrajConverted.createOrReplaceTempView("convGeoTrajFiltered")
      //spark.catalog.cacheTable("convGeoTrajFiltered")
      //convExplodetrajConverted.show()


      //VECCHIA IMPLEMENTAZIONE DEL JOIN PER DISTANCE DOVE NON FILTRAVO - 10K PUNTI IN 40 MINUTI
      /*val queryDF2 = spark.sql(
        """SELECT * FROM
          |(SELECT *,1/distance AS invertedDist, ROW_NUMBER() OVER (PARTITION BY a.customid,a.trajid,a.timest ORDER BY a.distance) AS ranko FROM
          |(SELECT *, ST_Distance(tf.geoPoint,gr.geoElement) AS distance FROM convGeoRoadDF gr CROSS JOIN convGeoTrajFiltered tf)a)b
          |WHERE b.ranko <=20""".stripMargin)
      queryDF2.createOrReplaceTempView("geoRoadForEachPoint")
      //queryDF.show(100)
      val queryDF3 = spark.sql("""SELECT *, a.invertedDist/a.sumDist AS prob FROM(SELECT *, SUM(invertedDist) OVER(PARTITION BY customid,trajid,timest) AS sumDist FROM geoRoadForEachPoint) a """)
      queryDF3.show(100,false)*/
      //val incorporateInfo = queryDF3.withColumn("roadSelected", struct("roadID","roadName","geoElement","ranko","distance","prob"))
      //val gruppones = incorporateInfo.groupBy(col("customid"),col("trajid"),col("timest"),col("latitude"),col("longitude")).agg(collect_list("roadSelected").as("roadsCandidateForPoint"))
      //val incorporateInfoPartTwo = gruppones.withColumn("pointWithRoads", struct("timest","latitude","longitude","roadsCandidateForPoint"))
      //val grupponespartwo = incorporateInfoPartTwo.groupBy(col("customid"),col("trajid")).agg(sort_array(collect_list("pointWithRoads")).as("trajectoryWithRoads"))


      //QUERY CON CUI CALCOLO LE 10 STRADE CANDIDATE PER OGNI PUNTO DI OGNI TRAIETTORIA, E CALCOLO ANCHE LA PROBABILITà DATA DA 1/DISTANZA/SOMMATORIA(1/DISTANZA)DI TUTTE LE STRADE CANDIDATE AL PUNTO
      val queryDF = spark.sql(
        ("""SELECT c.roadID,c.customid,c.trajid,c.timest,c.latitude,c.longitude, c.invertedDist/c.sumDist AS prob
          |FROM (
          |    SELECT b.roadID,b.customid,b.trajid,b.timest,b.latitude,b.longitude, 1/b.distance AS invertedDist,
          |        ROW_NUMBER() OVER (PARTITION BY b.customid, b.trajid, b.timest ORDER BY b.distance) AS ranko,
          |        SUM(1/b.distance) OVER(PARTITION BY b.customid, b.trajid, b.timest) AS sumDist
          |    FROM (
          |        SELECT gr.roadID,tf.customid,tf.trajid,tf.timest,tf.latitude,tf.longitude, CASE WHEN ST_Distance(tf.geoPoint, gr.geoElement) > 5 THEN ST_Distance(tf.geoPoint, gr.geoElement) ELSE 5 END AS distance
          |        FROM convGeoRoadDF gr JOIN convGeoTrajFiltered tf ON ST_Distance(tf.geoPoint, gr.geoElement) <= """+PAR_TAU+"""
          |    ) b
          |) c
          |WHERE c.ranko <= """+PAR_ALPHA+""" """).stripMargin).cache() //ENRICO tau e alpha??
      /*CALCOLO VICINATO CON CELLE, SHUFFLE MINORE
        val top10streetForeachCell = spark.sql(
          """SELECT c.roadID,c.geoElement,c.pointInfo,c.latitude,c.longitude
            |FROM (
            |SELECT b.roadID,b.geoElement,b.pointInfo,b.latitude,b.longitude,ROW_NUMBER() OVER (PARTITION BY b.latitude, b.longitude ORDER BY b.distance) AS ranko
            |    FROM (
            |        SELECT gr.roadID,gr.geoElement,tf.pointInfo,tf.latitude,tf.longitude, ST_Distance(tf.geoPoint, gr.geoElement) AS distance
            |        FROM convGeoRoadDF gr JOIN convGeoTrajFiltered tf ON ST_Distance(tf.geoPoint, gr.geoElement) <= 200
            |    ) b
            |)c
            |WHERE c.ranko <= 10""".stripMargin)

        //RAGGRUPPO TUTTE LE STRADE PER OGNI PUNTO E TUTTI I PUNTI PER OGNI TRAIETTORIA
        val explodeCellInPoint = top10streetForeachCell.select(col("roadID"),col("geoElement"),explode(col("pointInfo")).as("tmp")).select(col("roadID"),col("geoElement"),col("tmp.customid").as("customid"),col("tmp.trajid").as("trajid"),col("tmp.timest"),col("tmp.realLatitude").as("latitude"),col("tmp.realLongitude").as("longitude"),col("tmp.geoPoint").as("geoPoint"))
        explodeCellInPoint.createOrReplaceTempView("top10street")

        val calcProb = spark.sql(
          """
            |SELECT c.roadID,c.customid,c.trajid,c.timest,c.latitude,c.longitude, c.invertedDist/c.sumDist AS prob
            |FROM (
            |    SELECT b.roadID,b.customid,b.trajid,b.timest,b.latitude,b.longitude, 1/b.distance AS invertedDist,
            |        SUM(1/b.distance) OVER(PARTITION BY b.customid, b.trajid, b.timest) AS sumDist
            |    FROM (
            |        SELECT roadID,customid,trajid,timest,latitude,longitude, CASE WHEN ST_Distance(geoPoint, geoElement) > 5 THEN ST_Distance(geoPoint, geoElement) ELSE 5 END AS distance
            |        FROM top10street
            |    ) b
            |) c""".stripMargin)
      */

      queryDF.count() // ENRICO STEP A

      //CALCOLO VICINATO TRA STRADE CON SPARKSQL E TENTATIVI CON GEOSPARKSQL
      //queryDF.cache() //RIUTILIZZANDO QUESTO ELEMNTO SIA PER STRADE CANDIDATE CHE POI PER VITERBI LO CACHO PER NON RICALCOLARLO DUE VOLTE
      //queryDF.count()
      val segmentRoadInterest = queryDF.select(col("roadID")).distinct().repartition(numRepartition)
      //val roadDFcount = roadDF.count()
      //val firstJoinCount = firstJoin.count()
      //val filterDistinctCount = segmentRoadInterest.count()
      val segmentNeighborhood = dfDist.join(segmentRoadInterest,firstJoin("roadID")===segmentRoadInterest("roadID")).select(firstJoin("*"))
      //val postJoinCount = segmentNeighborhood.count()
      //println("roadDf: "+ roadDFcount + " firstJoin: "+ firstJoinCount+ " filteredroadDF: "+ filterDistinctCount+ " postJoin: "+ postJoinCount)
      //val segmentNeighborhood = convGeoRoadDF.join(segmentRoadInterest,convGeoRoadDF("roadID")===segmentRoadInterest("roadID")).select(convGeoRoadDF("roadID"),convGeoRoadDF("lenght"),convGeoRoadDF("geoElement"))
      //segmentNeighborhood.count()
      //segmentNeighborhood.show()
      val deepSearch = PAR_THETA // ENRICO theta
      val distanceSoglia = PAR_GAMMA // ENERICO gamma
      segmentNeighborhood.createOrReplaceTempView("segsSelectedNeigh")
      //spark.sqlContext.cacheTable("segsSelectedNeigh")
     /* val createPaths1 = spark.sql(createOmegaJoin("segsSelectedNeigh","completeNeighborhood",2))
      createPaths1.show(500)
      val createPaths2 = spark.sql(createOmegaJoin("segsSelectedNeigh","completeNeighborhood",3))
      createPaths2.show(500)*/
      //val createPaths3 = spark.sql(createOmegaJoin("segsSelectedNeigh","completeNeighborhood",4))
      //createPaths3.show(500)
      /*val createPaths4 = spark.sql(createOmegaJoin("segsSelectedNeigh","completeNeighborhood",5))
      createPaths4.show(500)
      val createPaths5 = spark.sql(createOmegaJoin("segsSelectedNeigh","completeNeighborhood",6))
      createPaths5.show(500)

      createPaths.show(500)*/
      //val createPaths = spark.sql("SELECT * FROM segsSelectedNeigh,geoRoadDF WHERE ST_Distance(segsSelectedNeigh.geoElement,geoRoadDF.geoElement) <= 0")
      //createPaths.count()
      //createPaths.show(500)

      //val createPaths = spark.sql(createOmegaLeftJoin("segsSelectedNeigh","completeNeighborhood",deepSearch,distanceSoglia))
      val createPaths = spark.sql(createOmegaLeftJoin("completeNeighborhood","completeNeighborhood",deepSearch,distanceSoglia)).cache()

    createPaths.count() // ENRICO STEP PRECALCOLO

      //val pino = segmentNeighborhood.join(broadcast(firstJoin),segmentNeighborhood("roadIDAttigua") === firstJoin("roadID"),"leftouter")

      val allPossibilities = createPaths.rdd.map(row=>{
        var listPoss:List[List[(String,Double)]] = List()
        var temp:List[(String,Double)] = List()
        for(i <- 0 until (deepSearch+1)*2 by 2) {
          if(row.get(i).asInstanceOf[String] != null) {
            temp = temp :+ (row.get(i).asInstanceOf[String], row.get(i + 1).asInstanceOf[Double])
            listPoss = listPoss :+ temp
          }
        }
        listPoss
      })

     //val flatAllPos = allPossibilities.flatMap(list=>list).groupBy(x=>(x.head._1,x.last._1)).mapValues(_.minBy(_.unzip._2.sum))
     val flatAllPos = allPossibilities.flatMap(list=>list).map(x=>((x.head._1,x.last._1),x)).aggregateByKey(List[(String,Double)](("de",100000)))((elementPalo, element)=> if(element.unzip._2.sum < elementPalo.unzip._2.sum)element else elementPalo, (elementPartizOne, elementPartizTwo) => if(elementPartizOne.unzip._2.sum < elementPartizTwo.unzip._2.sum) elementPartizOne else elementPartizTwo)
     val segAdjacencyMatrix = flatAllPos.map(x=>(x._1._1,x._1._2,x._2)).toDF("startID","endID","path").repartition(numRepartition,col("startID"),col("endID"))

    /*
     val adjacencyMap:Map[(String,String),List[(String,Double)]] = flatAllPos.collect().map(r=> {r._1 -> r._2}).toMap
     //val adjacencyMap = flatAllPos.collect().map(r=> {r._1._1 -> Map(r._1._2->r._2)}).toMap
     //println(adjacencyMap)
     //adjacencyMap.foreach(x=>println(x))
     val bstateProbabilitiesMatrix = spark.sparkContext.broadcast(adjacencyMap)
*/

      val incorporateRoadInfo = queryDF.withColumn("roadSelected", struct("roadID","prob"))
      val groupByTrajectoryPoint = incorporateRoadInfo.groupBy(col("customid"),col("trajid"),col("timest"),col("latitude"),col("longitude")).agg(collect_list("roadSelected").as("roadsCandidateForPoint"))
      val addToEachPointSegmentOfNextPoint = groupByTrajectoryPoint.withColumn("nextSegIDs",lead(col("roadsCandidateForPoint"),1).over(Window.partitionBy("customid","trajid").orderBy("timest")))
      //val explodeRoadPossibilities = addToEachPointSegmentOfNextPoint.withColumn("roadsCandidateForPoint",explode(col("roadsCandidateForPoint")).alias("segIDStart")).withColumn("nextSegIDs",explode(col("nextSegIDs")).alias("segIDEnd"))
      /*val explodeRoadPossibilities = addToEachPointSegmentOfNextPoint.rdd.flatMap(row => {
        row.getAs[Seq[Row]]("roadsCandidateForPoint").map(x=>
          row.getAs[Seq[Row]]("nextSegIDs").map( y=>
            (row.getAs[String]("customid"),row.getAs[Int]("trajid"),row.getAs[Int]("timest"),row.getAs[Double]("latitude"),row.getAs[Double]("longitude"),x.getAs[String]("roadID"),x.getAs[Double]("prob"),y.getAs[String]("roadID"))
        ))
      }).flatMap(x=>x).toDF("customid","trajid","timest","latitude","longitude","roadID","prob","succID")*/
      val explodeRoadPossibilities = addToEachPointSegmentOfNextPoint.rdd.flatMap(row => {
        val tmpOne= row.getAs[Seq[Row]]("roadsCandidateForPoint")
        val tmpTwo=  row.getAs[Seq[Row]]("nextSegIDs")
        if(tmpTwo == null){
          tmpOne.map(x=> (row.getAs[String]("customid"),row.getAs[Int]("trajid"),row.getAs[Int]("timest"),row.getAs[Double]("latitude"),row.getAs[Double]("longitude"),x.getAs[String]("roadID"),x.getAs[Double]("prob"),null))
        }else tmpOne.flatMap(x=>tmpTwo.map(y=> (row.getAs[String]("customid"),row.getAs[Int]("trajid"),row.getAs[Int]("timest"),row.getAs[Double]("latitude"),row.getAs[Double]("longitude"),x.getAs[String]("roadID"),x.getAs[Double]("prob"),y.getAs[String]("roadID"))))
      }).toDF("customid","trajid","timest","latitude","longitude","roadID","prob","succID")
      //explodeRoadPossibilities.take(50).foreach(println)
      val joinPathInfoToEachRoadPair = explodeRoadPossibilities.join(segAdjacencyMatrix,explodeRoadPossibilities("roadID")===segAdjacencyMatrix("startID")&& explodeRoadPossibilities("succID")===segAdjacencyMatrix("endID"),"left")
      //joinPathInfoToEachRoadPair.show()

      /*val explodeRoadPossibilities = addToEachPointSegmentOfNextPoint
        .as[(String, Int, Long,Double,Double, Seq[(String,Double)], Seq[(String,Double)])]
        .flatMap( row => row._6.map(x => row._7.map(y=> (row._1,row._2,row._3,row._4,row._5,x._1,x._2,y._1))))
      //.toDF("customid","trajid","timest","latitude","longitude","roadID","prob","endRoadID")
      explodeRoadPossibilities.show()
      val joinPathInfoToEachRoadPair = explodeRoadPossibilities.join(segAdjacencyMatrix,addToEachPointSegmentOfNextPoint("_6")===segAdjacencyMatrix("startID")&& addToEachPointSegmentOfNextPoint("_8")===segAdjacencyMatrix("endID"))
      joinPathInfoToEachRoadPair.show()
      */
      val incorporateRoadNewInfo = joinPathInfoToEachRoadPair.withColumn("roadSelected", struct("roadID","prob","succID","path"))
      val groupByTrajectorySecondTime = incorporateRoadNewInfo.groupBy(col("customid"),col("trajid"),col("timest"),col("latitude"),col("longitude")).agg(collect_list("roadSelected").as("roadsCandidateForPoint"))
      val incorporateAllPointInfoToTraj = groupByTrajectorySecondTime.withColumn("pointWithRoads", struct("timest","latitude","longitude","roadsCandidateForPoint"))
      val allTrajectoryWithAllInfoDF = incorporateAllPointInfoToTraj.groupBy(col("customid"),col("trajid"))
          .agg(sort_array(collect_list("pointWithRoads"))
          .as("trajectoryWithRoads"))
          .cache()
      allTrajectoryWithAllInfoDF.count() //ENRICO step B
      //grupponespartwo.show(200)

      //CREO IL CONVERTITORE E LO BROADCASTO PERCHè SARà USATO DENTRO AL MAP DELL'RDD
      val csName1 = "EPSG:4326"
      val csName2 = "EPSG:3857"
      val ctFactory = new CoordinateTransformFactory()
      val csFactory = new CRSFactory()
      val crs1 = csFactory.createFromName(csName1)
      val crs2 = csFactory.createFromName(csName2)
      val transTo3857 = ctFactory.createTransform(crs1, crs2)
      val transTo4326 = ctFactory.createTransform(crs2,crs1)
      val converterTo3857 = spark.sparkContext.broadcast(transTo3857)
      val converterTo4326 = spark.sparkContext.broadcast(transTo4326)


      /**
        * Metodo utilizzato per il calcolo aggiornato del path tra due strade candidate, considerando le proiezioni ortogonali dei punti su di esse
        * @param listPath percorso di segmenti
        * @param startPoint punto iniziale
        * @param endPoint punto finale
        * @return lunghezza percorso aggiornata
        */
      def newSegPathLenght(listPath:List[(String,Double)],startPoint:(Double,Double),endPoint:(Double,Double)):Double={
        //CONVERTO e TROVO PROIEZIONE
        var newSegPathLenght:Double = 0.0
        val initialSegCoords:((Double,Double),(Double,Double)) = bsegInFinMap.value(listPath.head._1)
        val finalSegCoords:((Double,Double),(Double,Double)) = bsegInFinMap.value(listPath.last._1)
        val pointOnFirstSegmentOfPath = orthogonalProj(new ProjCoordinate(initialSegCoords._1._1,initialSegCoords._1._2),
          new ProjCoordinate(initialSegCoords._2._1,initialSegCoords._2._2),
          new ProjCoordinate(startPoint._1,startPoint._2))
        val pointOnLastSegmentOfPath = orthogonalProj(new ProjCoordinate(finalSegCoords._1._1,finalSegCoords._1._2),
          new ProjCoordinate(finalSegCoords._2._1,finalSegCoords._2._2),
          new ProjCoordinate(endPoint._1,endPoint._2))
        //listPath.foreach(x=>println(x))
        //println("coordinata iniziale: "+ initialSegCoords._1+ " coordinata finale: "+ initialSegCoords._2)
        //println("pointOnFirstSegmentOfPath : "+pointOnFirstSegmentOfPath)
        //println("coordinata iniziale: "+ finalSegCoords._1+ " coordinata finale: "+ finalSegCoords._2)
        //println("pointOnLastSegmentOfPath : "+pointOnLastSegmentOfPath)
        //se ENTRAMBI non sono ortogonali, tengo la somma dei segmento del ListPath
        if(pointOnFirstSegmentOfPath.isEmpty && pointOnLastSegmentOfPath.isEmpty) newSegPathLenght = listPath.unzip._2.sum
        //caso un solo segmento, devo capire che estremo del segmento considerare per calcolare la distanza con il punto proiettato ortonogonalmente
        else if (listPath.length == 1){
          if(pointOnFirstSegmentOfPath.isEmpty){
            if(DistanceCalculatorImpl().calculateDistanceInMeter(Location(startPoint._1,startPoint._2),Location(initialSegCoords._1._1,initialSegCoords._1._2))< DistanceCalculatorImpl().calculateDistanceInMeter(Location(startPoint._1,startPoint._2),Location(initialSegCoords._2._1,initialSegCoords._2._2))){
              newSegPathLenght = DistanceCalculatorImpl().calculateDistanceInMeter(Location(initialSegCoords._1._1,initialSegCoords._1._2),Location(pointOnLastSegmentOfPath.get.lat,pointOnLastSegmentOfPath.get.lon))
            }else newSegPathLenght = DistanceCalculatorImpl().calculateDistanceInMeter(Location(initialSegCoords._2._1,initialSegCoords._2._2),Location(pointOnLastSegmentOfPath.get.lat,pointOnLastSegmentOfPath.get.lon))
          }else if(pointOnLastSegmentOfPath.isEmpty){
            if(DistanceCalculatorImpl().calculateDistanceInMeter(Location(endPoint._1,endPoint._2),Location(finalSegCoords._1._1,finalSegCoords._1._2))< DistanceCalculatorImpl().calculateDistanceInMeter(Location(endPoint._1,endPoint._2),Location(finalSegCoords._2._1,finalSegCoords._2._2))){
              newSegPathLenght = DistanceCalculatorImpl().calculateDistanceInMeter(Location(finalSegCoords._1._1,finalSegCoords._1._2),Location(pointOnFirstSegmentOfPath.get.lat,pointOnFirstSegmentOfPath.get.lon))
            }else newSegPathLenght = DistanceCalculatorImpl().calculateDistanceInMeter(Location(finalSegCoords._2._1,finalSegCoords._2._2),Location(pointOnFirstSegmentOfPath.get.lat,pointOnFirstSegmentOfPath.get.lon))
          }else{
            newSegPathLenght = DistanceCalculatorImpl().calculateDistanceInMeter(pointOnFirstSegmentOfPath.get,pointOnLastSegmentOfPath.get)
          }
        }
        //caso più segmenti
        else{
          var newListPath = listPath.unzip._2
          if(pointOnFirstSegmentOfPath.isDefined){
            var newLenghtFirstSeg:Double = 0.0
            if(initialSegCoords._1 == bsegInFinMap.value(listPath(1)._1)._1 || initialSegCoords._1 == bsegInFinMap.value(listPath(1)._1)._2){
              //println("NELL'IF dato: " +initialSegCoords._1 + "e nel segmento successivo c'è: "+bsegInFinMap.value(listPath(1)._1)._1+ "e :" +bsegInFinMap.value(listPath(1)._1)._2)
              newLenghtFirstSeg = DistanceCalculatorImpl().calculateDistanceInMeter(Location(initialSegCoords._1._1,initialSegCoords._1._2),Location(pointOnFirstSegmentOfPath.get.lat,pointOnFirstSegmentOfPath.get.lon))
            }else{
              //println("NELL ELSE dato: " +initialSegCoords._2 + "e nel segmento successivo c'è: "+bsegInFinMap.value(listPath(1)._1)._1+ "e :" +bsegInFinMap.value(listPath(1)._1)._2)
              newLenghtFirstSeg = DistanceCalculatorImpl().calculateDistanceInMeter(Location(initialSegCoords._2._1,initialSegCoords._2._2),Location(pointOnFirstSegmentOfPath.get.lat,pointOnFirstSegmentOfPath.get.lon))
            }
            //println("nuova lunghezza segmento iniziale: "+ newLenghtFirstSeg)
            newListPath = newListPath.updated(0,newLenghtFirstSeg)
          }
          if(pointOnLastSegmentOfPath.isDefined) {
            var newLenghtLastSeg:Double = 0.0
            if (finalSegCoords._1 == bsegInFinMap.value(listPath(listPath.length-2)._1)._1 || finalSegCoords._1 == bsegInFinMap.value(listPath(listPath.length-2)._1)._2) {
              //println("NELL'IF dato: " + finalSegCoords._1  + "e nel segmento successivo c'è: " + bsegInFinMap.value(listPath(listPath.length-2)._1)._1 + "e :" + bsegInFinMap.value(listPath(listPath.length-2)._1)._2)
              newLenghtLastSeg = DistanceCalculatorImpl().calculateDistanceInMeter(Location(finalSegCoords._1._1, finalSegCoords._1._2), Location(pointOnLastSegmentOfPath.get.lat, pointOnLastSegmentOfPath.get.lon))
            } else {
              //println("NELL ELSE dato: " + finalSegCoords._2 + "e nel segmento successivo c'è: " + bsegInFinMap.value(listPath(listPath.length-2)._1)._1 + "e :" + bsegInFinMap.value(listPath(listPath.length-2)._1)._2)
              newLenghtLastSeg = DistanceCalculatorImpl().calculateDistanceInMeter(Location(finalSegCoords._2._1, finalSegCoords._2._2), Location(pointOnLastSegmentOfPath.get.lat, pointOnLastSegmentOfPath.get.lon))
            }
            //println("nuova lunghezza segmento finale: "+ newLenghtLastSeg)
            newListPath = newListPath.updated(newListPath.length-1,newLenghtLastSeg)
          }
          //newListPath.foreach(x=>println(x))
          newSegPathLenght = newListPath.sum
        }
        //println("nuova lunghezza totale percorso : "+newSegPathLenght)
        newSegPathLenght
      }

      //metodo che calcola la proiezione ortogonale punto segmento e successivamente la lunghezza punto nuovo sul segmento/segmento
      def orthogonalProj(initialPointLine:ProjCoordinate,finalPointLine:ProjCoordinate, point:ProjCoordinate):Option[Location] ={
        val convertedInitialPointLine = new ProjCoordinate()
        //println("initialPointLine: "+initialPointLine)
        converterTo3857.value.transform(initialPointLine, convertedInitialPointLine)
        //println("convertedInitialPointLine: "+convertedInitialPointLine)
        val convertedFinalPointLine = new ProjCoordinate()
        //println("finalPointLine: "+finalPointLine)
        converterTo3857.value.transform(finalPointLine, convertedFinalPointLine)
        //println("convertedFinalPointLine: "+convertedFinalPointLine)
        val convertedPoint = new ProjCoordinate()
        //println("point: "+point)
        converterTo3857.value.transform(point, convertedPoint)
        //println("convertedPoint: "+convertedPoint)
        var  resultOrth = DistanceCalculatorImpl().pointOnLine(Location(convertedInitialPointLine.x,convertedInitialPointLine.y),Location(convertedFinalPointLine.x,convertedFinalPointLine.y),Location(convertedPoint.x,convertedPoint.y))
        if (resultOrth.isDefined){
          val convertResult = new ProjCoordinate()
          converterTo4326.value.transform(new ProjCoordinate(resultOrth.get.lat,resultOrth.get.lon),convertResult)
          resultOrth = Option(Location(convertResult.x,convertResult.y))
        }
        resultOrth
      }

      //PER OGNUNA DELLE TRAIETTORIE (RIGA DELL'RDD COMPOSTA DA CUSTOMID,TRAJID,GRUPPO DI PUNTI(GRUPPO DI STRADE CANDIDATE PER PUNTO)) ATTUO VITERBI
      val resutlViterbis = allTrajectoryWithAllInfoDF.rdd.flatMap(z=>{
      //grupponespartwo.collect().foreach(z=>{
        //var resultListTuples:ListBuffer[(String,Int,Int,String)] = ListBuffer[(String,Int,Int,String)]()
        var emissionStateMatrix: Map[String, Map[String, Double]] = Map()
        var initialStateDistribution:Map[String,Double] = Map()
        var stateMatrix:Map[Int,ListBuffer[String]] = Map()
        var observations = ListBuffer[String]()
        var obsNum = ListBuffer[Int]()
        var obsMap = ListBuffer[(Double,Double)]()
        var adjacencyMap:Map[(String,String),List[(String,Double)]] = Map()

        //ESTRAGGO TUTTE LE INFO E CREO LE STRUTTURE ADATTE
        val customid:String = z.getAs[String]("customid")
        val trajid:Int = z.getAs[Int]("trajid")
        var numberOfPointTrajCounter = 0
        val trajectoryWithCandidateRoads:Seq[Row] = z.getAs[Seq[Row]]("trajectoryWithRoads")
        val parametroRumoreGPS =10 //ENRICO beta??
        trajectoryWithCandidateRoads.foreach( x=> {
          val timest:Int = x.getAs[Int]("timest")
          val latitude:Double = x.getAs[Double]("latitude")
          val longitude:Double = x.getAs[Double]("longitude")
          var roadListForObservation = ListBuffer[String]()
          observations += customid+trajid+timest
          obsMap += ((latitude,longitude))
          obsNum += timest
          //println("Timestamp punto: "+timest+" con latitudine: "+latitude+" e longitudine: "+ longitude)
          x.getAs[Seq[Row]]("roadsCandidateForPoint").foreach(y=>{
            val roadID:String = y.getAs[String]("roadID")
            val succID:String = y.getAs[String]("succID")
            val rowpath = y.getAs[Seq[Row]]("path")
            if(rowpath != null) {
              val path = rowpath.map(x=>(x.getAs[String]("_1"),x.getAs[Double]("_2")))
              adjacencyMap += ((roadID,succID) -> path.toList)
            }
            val prob:Double = y.getAs[Double]("prob")
            //CALCOLO PROBABILITà PAPER 2009
            //val prob = (1/(Math.sqrt(2*math.Pi)*parametroRumoreGPS))*Math.pow(Math.E,-0.5*Math.pow(y.getAs[Double]("distance")/parametroRumoreGPS,2))
            //println("probabilità per strada: "+roadID+" con distanza: "+y.getAs[Double]("distance")+" è: "+prob)
            roadListForObservation += roadID
            //println("roadID: "+roadID + " roadName: "+ roadName + " rank: "+ rank + " distance: "+ distance + " probabilità: "+prob)
            if(emissionStateMatrix.contains(roadID)){
              var temp = emissionStateMatrix(roadID)
              temp += (customid+trajid+timest -> prob)
              emissionStateMatrix += (roadID->temp)
            }else{
              emissionStateMatrix += roadID -> Map(customid+trajid+timest -> prob)
            }
            if(!initialStateDistribution.contains(roadID)) initialStateDistribution += (roadID -> 0)
          })
          stateMatrix += numberOfPointTrajCounter -> roadListForObservation
          numberOfPointTrajCounter += 1
        })
        //PROBABILITà STATI INIZIALI EQUIVALENTI COME DA PAPER
        val k = 1.0f/10 // ENRICO perchè 1/10 ???
        initialStateDistribution = initialStateDistribution.map(m=>(m._1,k.toDouble))
        //initialStateDistribution.foreach(x=>println(x))
        //emissionStateMatrix.foreach(x=>println(x))
        //stateMatrix.foreach(x=>println(x))



        // 1/beta * e alla - valore assoluto(haversine tra i punti - sommatoria percorso considerando le proiezioni dei punti nei segmenti iniziali)
        val Betadafare:Double = PAR_BETA //𝛽 = 1/ln(2) mediant 􀀲􀀟‖𝑧𝑡 − 𝑧𝑡+1‖𝑔𝑟𝑒𝑎𝑡 𝑐𝑖𝑟𝑐𝑙𝑒− 􀀌𝑥𝑡,𝑖∗ − 𝑥𝑡+1,𝑗 ∗􀀌𝑟𝑜𝑢𝑡𝑒 􀀯􀀳 //ENRICO beta??
        var newStateProbMatrix:Map[(Int,String),Map[String,Double]] = Map()
        //PER OGNI OSSERVAZIONE FINO ALLA PENULTIMA
        for(i <- 0 to obsMap.length-2){
          val pointDist = DistanceCalculatorImpl().calculateDistanceInMeter(Location(obsMap(i)._1,obsMap(i)._2),Location(obsMap(i+1)._1,obsMap(i+1)._2))
          //if(i==114) println("distanza : "+pointDist)
          //GIRO PER OGNI SEGMENTO CANDIDATO DELL'OSSERVAZIONE E PER OGNUNO DI QUELLO SUCCESSIVO E AGGIORNO LE DISTANZE DEL PATH TRA DI LORO SECONDO PROIEZIONE ORTOGONALE
          stateMatrix(i).foreach(segment=>{
            var valueContainerTemp:Map[String,Double] = Map()
            stateMatrix(i+1).foreach(segmentSucc => {
              if(adjacencyMap.contains((segment,segmentSucc))) {
                //val listPath = bstateProbabilitiesMatrix.value(segment)(segmentSucc)._2
                val listPath = adjacencyMap((segment,segmentSucc))
                val segLenghtSum = newSegPathLenght(listPath,(obsMap(i)._1,obsMap(i)._2),(obsMap(i+1)._1,obsMap(i+1)._2))
                //if(i==114) println("segLenghtSum : " + segLenghtSum)
                //CALCOLO PROBABILITA' CON VALORI AGGIORNATI COME DA PAPER
                val stateTransProb = (1/Betadafare)*Math.pow(Math.E,(-Math.abs(pointDist-segLenghtSum))/Betadafare)
                //if(i==114) println("stateTransProb : " + stateTransProb+" dato punto: "+i+" con strada "+segment+" e punto successivo "+ i+1 +"con strada "+segmentSucc+" data da pointDist di: "+ pointDist+" e da sumLenght: "+segLenghtSum)
                if (stateTransProb < 0) valueContainerTemp += segmentSucc -> 0
                else valueContainerTemp += segmentSucc -> stateTransProb
              }else valueContainerTemp += segmentSucc -> 0
            })
            newStateProbMatrix += (i,segment) -> valueContainerTemp
          })
        }

        //println("count: "+count+ "matrixCount "+newStateProbMatrix.keySet.toList.length)
        //newStateProbMatrix.foreach(x=>println(x))

        //GENERO IL MODELLO E LO FACCIO PARTIRE
        val model = new HmmModel(
          newStateProbMatrix,
          emissionStateMatrix,
          initialStateDistribution,
          initialStateDistribution.toList.length)
        val result = AlgoViterbi.run(model,observations,stateMatrix)

        var realResult = ListBuffer[(Int,String)]()


        if(!result.isEmpty) {
          //RICOSTRUZIONE PATH!
          //A LOGICA è: PRENDO DATI DUE SEGMENTI DEL VITERBI PATH, IL LORO COLLEGAMENTO DALLA MATRICE, ESCLUDENDO IL SEGMENTO FINALE
          // CHE VERRà AGGIUNTO NEL CICLO SUCCESIVO, TRANNE IL CASO IL SEGMENTO SIA L'ULTIMO
          for (i <- result.indices) {
            //CASO ULTIMO PUNTO, AGGIUNGO IN FONDO SOLO QUELLO ED ESCO
            if (result(i) != null) {
              if (i == result.length - 1) realResult = realResult += ((obsNum(i),result(i)))
              //CASO DUE PUNTI SU STESSO SEGMENTO, SALTO, SENNò PRENDO IL PATH
              else if (!result(i).equals(result(i + 1)) && adjacencyMap.contains(result(i),result(i+1))) {
                //println("dentro alla ricostruizone realResult: "+realResult+" deve aggiungere: "+bstateProbabilitiesMatrix.value(result(i))(result(i+1))._2.unzip._1.dropRight(1))
                //realResult = realResult ++ bstateProbabilitiesMatrix.value(result(i))(result(i + 1))._2.unzip._1.dropRight(1)
                val tmp = adjacencyMap(result(i),result(i + 1)).unzip._1.dropRight(1)
                val newRes = tmp.map(x=>(obsNum(i),x))
                realResult = realResult ++ newRes
                //println("post unione "+realResult)
              }
            }
          }
        }
        //realResult.foreach(x=>println(x))

        //result.indices.foreach(x=> {
          //println("Punto: "+ x + " strada: "+ result(x))
         // println("Punto: "+ x + " latitudine: "+(obsMap(x)._1+" longitudine: "+obsMap(x)._2+" strada risultato: "+ result(x)))
        //})

        //RITORNO PER OGNI ROW DELL'RDD IL PATH RICORSTRUITO
        //(customid,trajid,realResult)
        println(customid+" "+trajid+" "+realResult)
        realResult.map(y=>(customid,trajid,y._1,y._2))
        //resultListTuples
      }).cache()
      resutlViterbis.count() // ENRICO STEP C

      //SALVATAGGIO RISULTATO MAPMATCHING SU TABELLA HIVE
      val resultWithColumnNames: DataFrame = resutlViterbis.toDF("customid","trajid","timest","roadID")
      //println(dfWithColNames.count())
      //dfWithColNames.show(false)
      //val explodeNodesToPrint = dfWithColNames.withColumn("singleRoad",explode(dfWithColNames("roads"))).select("customid", "trajid","singleRoad._1","singleRoad._2")
      //println(explodeNodesToPrint.count())

      //RECUPERO LE INFO PER OGNI STRADA
      resultWithColumnNames.write.mode(SaveMode.Append).saveAsTable(tablename)

      /*
      //SALVATAGGIO RISULTATO MAPMATCHING SU FILE CSV
      //PRENDO TUTTI I RISULTATI, LI METTO IN UN DATAFRAME PERCHè DEVO RECUPERARE LE INFO SULLE STRADE (AVENDO SOLO L'ID DI ESSE DA VITERBI)
      val finalTableToPrint = explodeNodesToPrint.join(roadDF,explodeNodesToPrint("singleRoad._1") === roadDF("roadID")).select(explodeNodesToPrint("customid"),explodeNodesToPrint("trajid"),explodeNodesToPrint("singleRoad"),roadDF("coordinates"))
      //finalTableToPrint.show(false)
      //CON POSEXPLODE ESPLODO ANCORA LA LISTA DELLE COORDINATE E GLI DO UN ORDINE, PER RIUSCIRE A VISUALIZZARLE CORRETTAMENTE SU TABLEAU
      val resultContainer = finalTableToPrint.select(finalTableToPrint("*"), posexplode(finalTableToPrint("coordinates"))).withColumn("latitude",col("col").getItem(0)).withColumn("longitude",col("col").getItem(1)).select(finalTableToPrint("customid"),finalTableToPrint("trajid"),finalTableToPrint("singleRoad"),col("latitude"),col("longitude"),col("pos").as("coordNumberInsideRoad"))
      //resultContainer.show(false)

      //SCRIVO SU HDFS IL RISULTATO
      resultContainer.write.format("com.databricks.spark.csv").save("/output")
      */
}}
