import scala.math._
import java.math.BigDecimal

import scala.collection.mutable.ListBuffer;

/**
  * Utility object.
  */
object Utils {

  object MyMath {
    def roundAt(n: Double, pos: Int): Double = {
      // new BigDecimal(n).setScale(pos, BigDecimal.ROUND_HALF_UP).doubleValue()
      val s = math pow (10, pos)
      (math floor n * s) / s
    }
  }

  /**
    * Distance estimation.
    */
  object Distances {

    val deglen = 110.25 // km
    val R = 6372.8 //radius in km

    /**
      * Euclidean approximation of the Haversine distance.
      */
    def haversineEuclideanApproximation(lat1: Double, lon1: Double, lat2: Double, lon2: Double): Double = {
      val x = lat1 - lat2
      val y = (lon1 - lon2) * Math.cos(lat2)
      deglen * Math.sqrt(x * x + y * y)
    }

    /**
      * Haversine distance.
      */
    def haversine(lat1: Double, lon1: Double, lat2: Double, lon2: Double): Double = {
      val dLat = (lat2 - lat1).toRadians
      val dLon = (lon2 - lon1).toRadians
      val a = pow(sin(dLat / 2), 2) + pow(sin(dLon / 2), 2) * cos(lat1.toRadians) * cos(lat2.toRadians)
      val c = 2 * asin(sqrt(a))
      R * c
    }
  }

  /**
    * GeoJSON conversion.
    */
  object GeoJSON {
    def toPointGeoJSON(lon: Double, lat: Double, roundat: Int = 8): String =
      "{\"type\":\"Point\",\"coordinates\":[" + MyMath.roundAt(lon, roundat) + "," + MyMath.roundAt(lat, roundat) + "]}"
  }

}