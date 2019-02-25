import scala.collection.mutable
import scala.collection.mutable.ListBuffer

object AlgoViterbi {

  def run(model: HmmModel, observations: ListBuffer[String], stateMatrix:Map[Int,ListBuffer[String]]): Array[String] = {

    val numStates = model.getNumHiddenStates
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
     * Inizializzazione
     */
     // primo step, primo punto inizializzo con distribuzione iniziale 1/k con k strade candidate
     //stateMatrix(0).foreach(i => deltas = deltas + ((0,i)->(Pi(i) * B(i).getOrElse(observations.head,0.0))))
    stateMatrix(0).foreach(i => deltas = deltas + ((0,i)->(Math.log(Pi(i)) + Math.log(B(i).getOrElse(observations.head,0.0)))))

      /*
       * Itero per ogni osservazione lungo il tempo
       */
    (1 until numObserv).foreach(t => {

      /*
       * Itero tra gli stati candidati data l'osservazione
       */
      //PER OGNI STRADA CANDIDATA DELL'OSSERVAZIONE SOTTO ESAME..
      stateMatrix(t).foreach(i => {
        /*
         * Cerco la massima probabilità che è data dal passaggio da uno degli stati precedenti allo stato in esame
         */
        //var maxState = states.head
        //var maxProb = deltas(t-1,states.head) * A(states.head).getOrElse(i,0.0)
        //var maxProb = deltas(t-1,stateMatrix(t-1).head) * A((t-1,stateMatrix(t-1).head)).getOrElse(i,0.0)
        var maxProb = deltas(t-1,stateMatrix(t-1).head) + Math.log(A((t-1,stateMatrix(t-1).head)).getOrElse(i,0.0))
        var maxState = stateMatrix(t-1).head
        /*deltas.foreach(x=> {
          if(x._1._1 == t-1){
            val street = A.get((t-1,x._1._2))
            var tempProb = 0.0
            street match {
              case Some(value) => tempProb = x._2*value.getOrElse(x._1._2,0.0)
              case None =>
            }
           // val tempProb = x._2*A(x._1._2).getOrElse(i,0.0)
            if(tempProb > maxProb) {
              maxProb = tempProb
              maxState = x._1._2
            }
          }
        })*/
        //println("maxProb: "+maxProb)
        //(1 until numStates).foreach(j => {
        //GIRO PER GLI STATI(STRADE) DEL LIVELLO PRECEDENTE t-1
        stateMatrix(t-1).foreach(j => {
          //val prob = deltas(t - 1, states(j)) *A(states(j)).getOrElse(i,0.0)
          //val prob = deltas(t - 1, j) *A((t-1,j)).getOrElse(i,0.0)
          val prob = deltas(t - 1, j) + Math.log(A((t-1,j)).getOrElse(i,0.0))
          /*if(t == 113 ||t == 114 || t == 115) {
            println("per la sua strada candidata: "+ i + " con strada strada del livello precedente "+j+ " ho prob: "+ prob + " composta da delta: "+ deltas(t - 1, j)+ "e A(j)(i): "+A(t - 1, j).getOrElse(i,0.0))
          }*/
          //println("deltas con t dove fa -1: "+t+" con states(j): "+states(j)+" vale: "+deltas(t - 1, states(j))+" e A(states(j)(i) con i "+i+ " vale: "+A(states(j)).getOrElse(i,0.0)+" prob è: "+prob)
          if (prob > maxProb) {
            /*if(t == 113 ||t == 114 || t == 115) {
              println("per l'osservazione "+ t + " per la sua strada candidata: "+ i + " trovata strada "+j+
               " con state transition prob di: "+ A((t-1,j)).getOrElse(i,0.0) + " che porta ad una prob di "+ prob+ " che supera "+ maxProb)
            }*/

            //println("per l'osservazione "+ observations(t)+ " per la sua strada candidata: "+ i + " trovata strada "+j+ " che supera "+ maxState)
            maxProb = prob
            maxState = j

          }
        })

        //deltas = deltas + ((t, i) ->(maxProb*B(i).getOrElse(observations(t),0.0)))
        deltas = deltas + ((t, i) ->(maxProb + Math.log(B(i).getOrElse(observations(t),0.0))))
        /*if(t==113||t == 114 || t == 115) {
          println("per l'osservazione: "+t+" dato il segmento "+i+" il suo maxstate degli stati precedenti è: "+ maxState+ " con probabilità: "+ maxProb+ " calcola delta composto da prob emissione: "+B(i).getOrElse(observations(t),0.0))
        }*/
        //println("maxState: "+maxState)
        phis = phis + ((t-1,i)->maxState)
        //println("per l'osservazione "+ observations(t)+ " per la sua strada candidata: "+ i + " il meglio collegato con le strade precenti è "+ maxState + " e ha prob: " +maxProb*B(i).getOrElse(observations(t),0.0))

      })

    })


    /*
     * Cerco lo stato finale con maggiore prob
     */
    //var maxProb = 0.0
    var maxProb = Double.NegativeInfinity
    var previous:String = ""

    //PRENDO LO STATO FINALE CON PROB MAGGIORE (QUINDI DAL LIVELLO DELLE STRADE CANDIDATE DELL'ULTIMO PUNTO DELLA TRAIETTORIA)
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

    //BACKTRACKING DALL'ULTIMO STATO FINO ALL'INIZIO
    if(exitWithoutFounding){
      sequence(0) = "STRADA SENZA MATCHING"
    }else{
      //println("dentro al recurse con stateMatrixSize: "+ stateMatrixSize)
      for (t <- stateMatrixSize - 1 to 0 by -1) {
        sequence(t) = phis(t,sequence(t + 1))

      }
    }

    /*stateMatrix(numObserv-1).foreach(i => {
      if (deltas(observations.length - 1,i) > maxProb) {
        maxProb = deltas(observations.length - 1,i)
        sequence(observations.length - 1) = i
        previous = i
      }

    })*/

    /*
     * Backtrack to find the most likely hidden sequence
     */
    /*for (t <- observations.length - 2 to 0 by -1) {
      sequence(t) = phis(t,sequence(t + 1))

    }*/
  }

}
