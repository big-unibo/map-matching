/**
  * class that represent a Hidden Markov Model
  */
class HmmModel {


  /* number of hidden states foreach observation*/
  private var numHiddenStates:Int = 0

  /*
   * matrix that represent transition matrix
   */
  private var A:Map[(Int,String),Map[String,Double]] = _

  /*
   * matrix that represent emission matrix
   */
  private var B:Map[String,Map[String,Double]] = _

  /*
   * map that contains initial probabilities
   */
  private var Pi:Map[String,Double] = _


  /**
    * Generates a Hidden Markov Model with specified parameters
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
