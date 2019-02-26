import scala.collection.mutable
import scala.collection.mutable.ListBuffer

/**
  *  object used to calculate Viterbi Algorithm
  */
object AlgoViterbi {

  /**
    * method that implements Viterbi Algorithm
    * @param model HMM model
    * @param observations observations list
    * @param stateMatrix matrix of candidate states of the observations
    * @return most likely path
    */
  def run(model: HmmModel, observations: ListBuffer[String], stateMatrix:Map[Int,ListBuffer[String]]): Array[String] = {

    val numObserv = observations.length

    /*
     * Probability that the most probable hidden states ends
     * at state i at time t
     */
    val delta: mutable.Map[(Int,String),Double] =mutable.Map[(Int,String),Double]()


    /*
     * Previous hidden state in the most probable state leading up
     * to state i at time t
     */
   val phi:mutable.Map[(Int,String),String] =mutable.Map[(Int,String),String]()

    val sequence = new Array[String](numObserv)
    run(sequence, delta, phi, model, observations,stateMatrix)

    sequence

  }

  def run(sequence:Array[String], delta:mutable.Map[(Int,String),Double], phi:mutable.Map[(Int,String),String], model:HmmModel, observations:ListBuffer[String], stateMatrix:Map[Int,ListBuffer[String]]) {

    val Pi = model.getPiVector

    val A = model.getAMatrix
    val B = model.getBMatrix

    val numObserv = observations.length

    var deltas = delta
    var phis = phi
    /*
     * INITIALIZATION
     */
    //FIRST STEP - first observation initilized with initial distribuition
    stateMatrix(0).foreach(i => deltas = deltas + ((0,i)->(Math.log(Pi(i)) + Math.log(B(i).getOrElse(observations.head,0.0)))))

    /*
     * foreach observation
     */
    (1 until numObserv).foreach(t => {

      /*
       * foreach candidate states of observation t
       */
      stateMatrix(t).foreach(i => {
        /*
         * Finding max joint porbability
         */
        var maxProb = deltas(t-1,stateMatrix(t-1).head) + Math.log(A((t-1,stateMatrix(t-1).head)).getOrElse(i,0.0))
        var maxState = stateMatrix(t-1).head
        stateMatrix(t-1).foreach(j => {
           val prob = deltas(t - 1, j) + Math.log(A((t-1,j)).getOrElse(i,0.0))
          if (prob > maxProb) {
            maxProb = prob
            maxState = j

          }
        })
        deltas = deltas + ((t, i) ->(maxProb + Math.log(B(i).getOrElse(observations(t),0.0))))
        phis = phis + ((t-1,i)->maxState)
      })

    })


    /*
     * Finding last state with highest probability
     */
    var maxProb = Double.NegativeInfinity
    var previous:String = ""
    
    var founded = false
    var exitWithoutFounding = false
    var stateMatrixSize = observations.length
    while (!founded ){
      stateMatrixSize -= 1
      stateMatrix(stateMatrixSize).foreach(i => {
        if (deltas(stateMatrixSize,i) > maxProb) {
          maxProb = deltas(stateMatrixSize ,i)
          sequence(stateMatrixSize) = i
          previous = i
          founded = true
        }
      })
      if(stateMatrixSize == 0 && !founded){
        founded = true
        exitWithoutFounding = true
      }
    }

    //backtracking to obtain most likely path
    if(exitWithoutFounding){
      sequence(0) = "road without matching"
    }else{
      for (t <- stateMatrixSize - 1 to 0 by -1) {
        sequence(t) = phis(t,sequence(t + 1))

      }
    }
  }

}
