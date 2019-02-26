import java.util.{Calendar, GregorianCalendar, Locale, TimeZone}

import org.apache.spark.sql.{SaveMode, SparkSession}

import scala.util.Random
import Utils.MyMath._
import Utils.GeoJSON
import org.apache.spark.serializer.KryoSerializer
import org.datasyslab.geospark.serde.GeoSparkKryoRegistrator
import org.datasyslab.geosparksql.utils.GeoSparkSQLRegistrator

import scala.collection.mutable.ListBuffer

/*
// spark2-submit  --files=/etc/hive/conf/hive-site.xml --driver-memory 10G --master yarn --deploy-mode client --class "it.unibo.cariplo.Enrich" cariplo.jar
// spark2-submit  --files=/etc/hive/conf/hive-site.xml --driver-memory 10G --num-executors 20 --executor-memory 10G --master yarn --deploy-mode client --class "it.unibo.cariplo.Enrich" cariplo.jar
spark2-submit \
      --driver-java-options "-Dhive.metastore.uris=thrift://isi-bigcluster1.csr.unibo.it:9083" \
      --files=/etc/hive/conf/hive-site.xml \
      --num-executors 20 \
      --executor-memory 10G \
      --conf="spark.driver.memory=15G" \
      --master yarn \
      --deploy-mode client \
      --class "TrajSegmentation" \
      /home/fvitali/prova.jar

      spark2-submit \
      --driver-java-options "-Dhive.metastore.uris=thrift://isi-bigcluster1.csr.unibo.it:9083" \
      --files=/etc/hive/conf/hive-site.xml \
      --master yarn \
      --deploy-mode local \
      --class "TrajSegmentation" \
      /home/fvitali/prova.jar

*/
object TrajSegmentation {
  def main(args: Array[String]): Unit = {
    val intablename = "cariploext"
    val tablename = "cariploenewfiltermilanonly"
    val maxtimediff = 60 // (seconds) to build a trajectory
    val maxspacediff = 500 //ora è in metri
    val maxspeediff = 80 // corrispondono a 22 m/s
    val minpoints = 50 //sicuro di non volerli abbassare a tipo 50 o 100?
    val round = 6 // esagerato 8 , puoi arrontadare tutto (lat e lon comprese) a 0.00001
    val msTokmh = 3.6

    val spark: SparkSession =
      SparkSession.builder().appName("newTrajSegmentation")
        .enableHiveSupport
        .config("spark.serializer", classOf[KryoSerializer].getName)
        .config("spark.kryo.registrator", classOf[GeoSparkKryoRegistrator].getName)
        .getOrCreate

    import spark.implicits._
    GeoSparkSQLRegistrator.registerAll(spark.sqlContext)

    val sc = spark.sparkContext
    // sc.setLogLevel("ERROR")
    // Logger.getLogger("org").setLevel(Level.ERROR)
    // Logger.getLogger("akka").setLevel(Level.ERROR)
    // LogManager.getRootLogger.setLevel(Level.ERROR)

    //val hiveContext = new HiveContext(sc)
    spark.sql("use trajectory")
    spark.sql("drop table if exists " + tablename)
    val sql =
      "select customid, timestamp, latitude, longitude, accuracy" +
        " from " + intablename +
        " where customid is not null and timestamp is not null and longitude is not null and latitude is not null and accuracy is not null" +
        " and timestamp between 1504224000 and 1514764800" +
        " and latitude between 45.38673 and 45.53586 and longitude between 9.04088 and 9.27812" + //da mettere per prendere solo confini milano
        " and accuracy < 100" // può essere ulteriormente ridotta, prendere sotto i 100
    println(sql)
    println("write to: " + tablename)
    spark.sql(sql)
      // .filter(col("customid").isNotNull).withColumn("accuracy", when(col("accuracy").isNotNull, col("accuracy")).otherwise(Integer.MAX_VALUE)) // replace null value
      .rdd
      .map(x => (x.getString(0), (x.getString(0), x.getInt(1), x.getDouble(2), x.getDouble(3), x.getInt(4))))
      .groupByKey // .sortBy(x => x._2.size, false).filter(_._2.size > minpts)
      .filter(_._2.size > minpoints) // consider only users with a sufficient number of points
      .flatMapValues(it => {
      val r = new Random(0)
      var trjid = 0
      val windowLenght = 5
      val halfWindow = 2
      var thr = 0.0
      val thrApiedi = 2
      //var isFirstPoint:Boolean = true
      var prev: (String, Int, Double, Double, Int, Double, Double,Double,Double) = ("foo", -Integer.MAX_VALUE, Integer.MAX_VALUE, Integer.MAX_VALUE, Integer.MAX_VALUE, Integer.MAX_VALUE, Integer.MAX_VALUE, Integer.MAX_VALUE, Integer.MAX_VALUE) // se mantenessi la lista di N punti precedenti, potrei fare una finestra per validare il punto corrente sulla base di quelli passati
      var listPrev:ListBuffer[(String, Int, Double, Double, Int, Double,Double,Int,Double)] = new ListBuffer[(String, Int, Double, Double, Int, Double,Double,Int,Double)]
      it.groupBy({ case (_: String, timest: Int, _: Double, _: Double, _: Int) => timest }) // group by time to remove duplicates (non togliere del tutto questo group by, potresti avere più punti nello stesso secondo, ti basta fare group by su timsest)
        .mapValues(l => l.minBy(_._5)) // choose the duplicate with the best accuracy
        .values.toSeq // get the values from the new map
        .sortBy(_._2) // sort them by time
        .map({ case (cid: String, timest: Int, lat: Double, lon: Double, acc: Int) => // qui mappo ogni punto, in cascata potrei aggiungere una filter per filtrare gli outlier
        var dtime: Int = timest - prev._2
        var isOutlier:Boolean = false
        var dspace: Double = DistanceCalculatorImpl().calculateDistanceInMeter(Location(lat,lon),Location(prev._3, prev._4))
        var speed: Double = (dspace / dtime)*msTokmh // calcolo già la distanza in metri, lo passo in km/h per maggiore leggibilità
        var dspeed: Double = speed - prev._6 //differenza velocità con punto precedente
        if (dtime < maxtimediff) { //se sono ancora in traiettoria
          if(listPrev.length == windowLenght) {
            listPrev.remove(0) //tolgo il primo elemento
            listPrev += ((cid, timest, lat, lon, acc, speed,dspeed,dtime,dspace)) //aggiungo in fondo
            val prePuntoMedia = (listPrev(halfWindow-1)._6 + listPrev(halfWindow-2)._6)/2 //calcolo media differenze speed
            val postPuntoMedia = (listPrev(halfWindow+1)._6 + listPrev(halfWindow+2)._6)/2
            thr = listPrev(halfWindow)._6/3 + thrApiedi //soglia impostato come velocità/2 + velocità considerata a piedi
            if(Math.abs(listPrev(halfWindow)._6-prePuntoMedia) > thr && Math.abs(listPrev(halfWindow)._6-postPuntoMedia) > thr){
              isOutlier = true
            }
          }else{
            listPrev += ((cid, timest, lat, lon, acc, speed,dspeed,dtime,dspace)) //aggiungo in fondo
            isOutlier = true
          }
          //controllo prima se è un outlier, se supera la soglia della differenza di velocità e non è il primo punto della traiettoria:
          //if(dspeed > maxspeediff && !isFirstPoint) isOutlier = true
          //da qui in poi non sono piu il primo punto della traiettoria
          //isFirstPoint = false
        }else {
          listPrev = new ListBuffer[(String, Int, Double, Double, Int, Double,Double,Int,Double)] //resetto lista window
          listPrev += ((cid, timest, lat, lon, acc, speed,dspeed,dtime,dspace)) //dopo il reset inserisco l'elemento della nuova traiettoria
          isOutlier = true
          dtime = 0
          dspace = 0
          dspeed = 0
          speed = 0
          trjid += 1
          //isFirstPoint = true //resetto la traiettoria, di conseguenza resetto il booleano
        }

        prev = (cid, timest, lat, lon, acc, speed,dspeed,dtime,dspace)
        //if(!isOutlier) prev = (cid, timest, lat, lon, acc, speed) //aggiorno prev solo se non è outlier
        
        val c = new GregorianCalendar(TimeZone.getTimeZone("Europe/Rome"), Locale.ITALY)
        if(listPrev.length == windowLenght){
          val customid = listPrev(halfWindow)._1
          val timest = listPrev(halfWindow)._2
          val lat = listPrev(halfWindow)._3
          val lon = listPrev(halfWindow)._4
          val acc = listPrev(halfWindow)._5
          val speed:Double = listPrev(halfWindow)._6
          val dspeed = listPrev(halfWindow)._7
          val dtime:Int = listPrev(halfWindow)._8
          val dspace = listPrev(halfWindow)._9
          c.setTime(new java.util.Date(timest * 1000L))
          (customid, timest, roundAt(lat, round), roundAt(lon, round), acc, dtime, roundAt(dspace, round), roundAt(speed, round), roundAt(dspeed, round),
            trjid, roundAt(r.nextDouble, round), c.get(Calendar.DAY_OF_WEEK), c.get(Calendar.HOUR_OF_DAY), GeoJSON.toPointGeoJSON(lon,lat, round), if (isOutlier) "out" else "in")
        }else {
          c.setTime(new java.util.Date(timest * 1000L))
          (cid, timest, roundAt(lat, round), roundAt(lon, round), acc, dtime, roundAt(dspace, round), roundAt(speed, round), roundAt(dspeed, round),
            trjid, roundAt(r.nextDouble, round), c.get(Calendar.DAY_OF_WEEK), c.get(Calendar.HOUR_OF_DAY), GeoJSON.toPointGeoJSON(lon, lat, round), if (isOutlier) "out" else "in")
        }
      })
    })
      .values
      .filter(_._15 == "in") //tengo solo i non outlier
      .toDF("customid", "timest", "latitude", "longitude", "accuracy", "timediff", "spacediff", "speed","speeddiff", "trajid", "rnd", "weekday", "dayhour", "geojson","status")
      .write.mode(SaveMode.Append)
      .saveAsTable(tablename)
    spark.close
  }
}



