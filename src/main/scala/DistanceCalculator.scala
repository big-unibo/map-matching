/**
  * Classe che rappresenta un punto GPS
  * @param lat latitudine del punto
  * @param lon longitudine del punto
  */
case class Location(lat: Double, lon: Double)

/**
  *
  */
trait DistanceCalculator {
  def calculateDistanceInMeter(userLocation: Location, warehouseLocation: Location): Double
  def pointOnLine(line1:Location, line2:Location, pt:Location):Option[Location]

  }

case class DistanceCalculatorImpl() extends DistanceCalculator {

    private val AVERAGE_RADIUS_OF_EARTH_KM = 6371

  /**
    * Metodo utilizzato per calcolare la distanza di Haversine
    * @param userLocation punto di partenza
    * @param warehouseLocation punto di fine
    * @return distanza in METRI tra i due punti
    */
    override def calculateDistanceInMeter(userLocation: Location, warehouseLocation: Location): Double = {
    val latDistance = Math.toRadians(userLocation.lat - warehouseLocation.lat)
    val lngDistance = Math.toRadians(userLocation.lon - warehouseLocation.lon)
    val sinLat = Math.sin(latDistance / 2)
    val sinLng = Math.sin(lngDistance / 2)
    val a = sinLat * sinLat +
      (Math.cos(Math.toRadians(userLocation.lat))
        * Math.cos(Math.toRadians(warehouseLocation.lat))
        * sinLng * sinLng)
    val c = 2 * Math.atan2(Math.sqrt(a), Math.sqrt(1 - a))
    (AVERAGE_RADIUS_OF_EARTH_KM * c)*1000
  }

  /**
    * Metodo che cerca la proiezione ortogonale del punto sulla retta
    * @param line1 inizio retta
    * @param line2 fine retta
    * @param pt punto che verrà proiettato sulla retta
    * @return option che contiene il punto se è possibile proiettare il punto ortonogonalmente, vuoto altrimenti
    */
  override def pointOnLine(line1:Location, line2:Location, pt:Location):Option[Location] = {
    var isValid:Boolean = false
    var newLine1 = Location(0,0)

    if (line1.lat == line2.lat && line1.lon == line2.lon) newLine1 = Location(line1.lat - 0.00001,line1.lon) else newLine1 = line1

    var U = ((pt.lat - newLine1.lat) * (line2.lat - newLine1.lat)) + ((pt.lon - newLine1.lon) * (line2.lon - newLine1.lon))

    val Udenom = Math.pow(line2.lat - newLine1.lat, 2) + Math.pow(line2.lon - newLine1.lon, 2)

    U /= Udenom

    val lat = newLine1.lat + (U * (line2.lat - newLine1.lat))
    val lon = newLine1.lon + (U * (line2.lon - newLine1.lon))
    val r = Location(lat, lon)

    val minx = Math.min(newLine1.lat, line2.lat)
    val maxx = Math.max(newLine1.lat, line2.lat)

    val miny = Math.min(newLine1.lon, line2.lon)
    val maxy = Math.max(newLine1.lon, line2.lon)

    isValid = (r.lat >= minx && r.lat <= maxx) && (r.lon >= miny && r.lon <= maxy)

    if(isValid) Option(r) else None
  }

}




