import java.util.Random

import scala.collection.mutable
import scala.collection.mutable.HashMap

/**
  */
class HmmModel {


  /* Numero degli stati nascosti per ogni osservazione*/
  private var numHiddenStates:Int = 0

  /*
   * Mappa di transizioni tra stati hidden
   */
  private var A:Map[(Int,String),Map[String,Double]] = _

  /*
   * Mappa di osservazione tra osservazione e stati candidati
   */
  private var B:Map[String,Map[String,Double]] = _

  /*
   * Mappa contenente i valori degli stati considerati iniziali
   */
  private var Pi:Map[String,Double] = _


  /**
    * Genera un HMM con i parametri specificati
    */
  def this(A:Map[(Int,String),Map[String,Double]],B:Map[String,Map[String,Double]],Pi:Map[String,Double],numHiddenStates:Int) {
    this()
    this.A = A
    this.B = B
    this.Pi = Pi
    this.numHiddenStates = numHiddenStates
  }


  def getAMatrix:Map[(Int,String),Map[String,Double]] = A

  def getBMatrix:Map[String,Map[String,Double]] = B

  def getPiVector:Map[String,Double] = Pi

  def getNumHiddenStates: Int = numHiddenStates

}
