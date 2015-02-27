
import org.apache.spark.SparkContext
import org.apache.spark.SparkContext._
import org.apache.spark.SparkConf
import java.lang.Math
import org.apache.spark.rdd.RDD



def exclude[T] (connections : Array[(String,String,T)], from: String, to :String) = {
  connections.filter( f=> f._1 != from && f._2 != to)
}

def excludeRDD[T] (connections : RDD[(String,String,T)], from: String, to :String) = {
  connections.filter( f=> f._1 != from && f._2 != to)
}


def minPlus(arr :Array[Double]) :Double= {

  if(arr.isEmpty)
  {
    0
  }
  else
  {
    arr.min
  }
}

def minPlusRDD(arr :RDD[Double]) :Double= {

  if(arr.isEmpty)
  {
    0
  }
  else
  {
    arr.min
  }
}


def reduce  (
connections : Array[(String, String, Double)],
allPoints : Array[String] )
= {
 //println(s"Accumulator for connections $connections")
 //println(s"SparkContex $sc")

 var lowerBound = 0.0 //sc.accumulator(0.0)

 //println("Before reduction by row")

   val reducedByRow = allPoints.flatMap {x1 =>
          val fromPoints = connections.filter(f => f._1 == x1)
          val min = minPlus(fromPoints.map(x2 => x2._3))
          lowerBound = lowerBound + min
          fromPoints.map{  x2 => (x2._1,x2._2, x2._3 - min) }
       }

//println("Before reduction by col")

  val reducedByRowAndCol = allPoints.flatMap {x1 =>
          val toPoints = reducedByRow.filter(f => f._2 == x1)
          val min = minPlus(toPoints.map(_._3))
          //lowerBound += min
          lowerBound = lowerBound + min
          toPoints.map{  x2 => (x2._1,x2._2, x2._3 - min) }
       }

  (reducedByRowAndCol, lowerBound)
}




def findNextPointCounted (
  movesToGo:Int,
  linksData :Array[(String, String, Double)] ,
  acc:Double,
  startPoint :String ,
   moves :List[(String,String)] ,
    allPoints :Array[String] ,
     searchPoints :Array[String],
     firstPoint : String
     )
    : (Double, List[(String,String)] ) = {

  //println(s"Moves to go: $movesToGo List: $linksData ACC: $acc")

  if(movesToGo== 0 || linksData.isEmpty )
  {
    //println(s"LinksData is empty acc is $acc and moves are: $moves")
    (acc, moves)
  }
  else
  {
  //  println(s"$movesToGo and list: $linksData Before reduction")
    val resultsOfRed2 =   linksData.filter(f=> f._1 ==  startPoint ) .map { x2=>
                val value = x2._3
                val newConnections  =exclude (linksData , x2._1, x2._2)
                (newConnections,  value , x2._2)
            }.map { x3 =>

              //println("Before reduction")

              val reduced = reduce(x3._1, searchPoints)
              //println("After reduction")

              (reduced._1, reduced._2 +  x3._2 + acc,  x3._3)
             }

               val searchForBest = resultsOfRed2.map { x =>
                   findNextPointCounted( if( movesToGo == -1 ) { 2 } else {movesToGo - 1} , x._1 , x._2, x._3,  ( startPoint, x._3) :: moves , allPoints, searchPoints, firstPoint )
                 }.toSeq.sortBy(_._1)

            val avaConnections = searchForBest(0)._1
            val avaAcc = searchForBest(0)._2

            // linksData subtraction avaConnection
            // move :: avaConnection
            // last step
            searchForBest(0)


  }
}


def findNextPoint (linksData :Array[(String, String, Double)] , acc:Double,  startPoint :String , moves :List[(String,String)] , allPoints :Array[String] , searchPoints :Array[String] ) : (Double, List[(String,String)] ) = {
      if(linksData.isEmpty )
      {
        println(s"LinksData is empty acc is $acc and moves are: $moves")
        val firstPoint = sc.makeRDD(allPoints).subtract( sc.makeRDD(searchPoints)).collect

        (acc, (startPoint, firstPoint(0) ) :: moves)
      }
      else
      {
        val resultsOfRed2 =   linksData.filter(f=> f._1 ==  startPoint ) .map { x2=>
                    val value = x2._3
                    val newConnections  =exclude (linksData , x2._1, x2._2)
                    (newConnections,  value , x2._2)
                }.map { x3 =>
                  val reduced = reduce(x3._1, searchPoints)
                  (reduced._1, reduced._2.value +  x3._2 + acc,  x3._3)
                 }


        val allPosibilites  = sc.parallelize( resultsOfRed2).map { x =>
            println(s"Procesing $x")
            findNextPointCounted( 2, x._1 , x._2, x._3,  ( startPoint, x._3) :: moves , allPoints, searchPoints, firstPoint )
          }.collect



        val accResults = resultsOfRed2.map(_._2)


        val bestRecords  = sc.parallelize( accResults.zipWithIndex).sortByKey().take(3) //   Math.max(Math.ceil(accResults.length * 0.6).toInt, 2))
        val allPosibilites =  bestRecords.map{x=>
            val result = resultsOfRed2(x._2)
            println(s"Best records are $result for $moves")
            findNextPointCounted( 3, result._1 , x._1, result._3,  ( startPoint, result._3) :: moves , allPoints, searchPoints )
            }

        sc.makeRDD(allPosibilites).filter(x => x._2(0)._2 == allPoints(0)).sortBy(x=>x._1).first
      }
}

def countPath (pathLink : List[(String,String)], linksData : Array[(String,String,Double)] ) = {
  val pathRDD = sc.makeRDD(pathLink)
  pathRDD.aggregate(0.0)( (a,v) =>{
      val finded =   linksData.find(f=> f._1 == v._1 && f._2 == v._2);
      a + (finded.get._3)
    },
    (a1,a2) => a1 + a2 )
}



def solveTspBab (linksData : Array[(String,String,Double)] )={
     println (s"Trying to solve $linksData")

     val linksRDD = sc.parallelize(linksData)
     val from = linksRDD.map(_._1)
     val to = linksRDD.map(_._2)
     val allPoints = from.union(to).distinct.collect
     val searchPoints = allPoints.filter(_ != allPoints(0))

     val path = findNextPoint( linksData, 0.0 , allPoints(0), List(), allPoints, searchPoints)

     (path._2, countPath(path._2, linksData))
}


/////////

val linksData1 : Array[(String, String, Double)] = Array(
  ("a", "b", 20),
  ("a", "c", 30),
  ("a", "d", 10),
  ("a", "e", 11),

  ("b", "a", 15),
  ("b", "c", 16),
  ("b", "d", 4),
  ("b", "e", 2),

  ("c", "a", 3),
  ("c", "b", 5),
  ("c", "d", 2),
  ("c", "e", 4),

  ("d", "a", 19),
  ("d", "b", 6),
  ("d", "c", 18),
  ("d", "e", 3),

  ("e", "a", 16),
  ("e", "b", 4),
  ("e", "c", 7),
  ("e", "d", 16)

 )

 val linksData2 : Array[(String, String, Double)]  = Array(
   ("a", "b", 10),
   ("a", "c", 8),
   ("a", "d", 9),
   ("a", "e", 7),

   ("b", "a", 10),
   ("b", "c", 10),
   ("b", "d", 5),
   ("b", "e", 6),

   ("c", "a", 8),
   ("c", "b", 10),
   ("c", "d", 8),
   ("c", "e", 6),

   ("d", "a",9),
   ("d", "b",5),
   ("d", "c",8),
   ("d", "e",6),

   ("e", "a", 7),
   ("e", "b", 6),
   ("e", "c", 9),
   ("e", "d", 6)

  )

////////




def readTspFile (sc :SparkContext , path :String, skip :Int) = {
    println(s"Reading file $path")
    val lines = sc.textFile(path, 1)
    val links = lines
    .mapPartitionsWithIndex { (idx, iter) => if (idx <= skip) iter.drop(skip) else iter }
    .map( s => (s,  s.split("\\s+") ))
    .filter{ case (s,p) => p.length > 2 }
    .map{ case (s,p) =>
          (p(1), p(2))
    }.distinct() //.cache() //.groupByKey()

    links
}

def mkConnections (sc :SparkContext, positions: RDD[ (String, String) ]) = {
   positions.map ( _.toString()).cartesian(positions.map(x=>x.toString())).filter { case (a,b) => a != b}
}

def PairPermutationsOf(n:Long) = {
  n * (n - 1)
}

def printlnPoint (arr :Array[String]) ={
  println("X=" + arr(0) + "  Y=" +arr(1))
}

def cleanStr (str:String) = {
  str.replace(")","").replace("(","")
}

def distance (p :(String,String)) ={
  val seperator = ","
  val p1Pos = cleanStr(p._1).split(seperator)
  //printlnPoint(p1Pos)
  val p2Pos = cleanStr(p._2).split(seperator)
  //printlnPoint(p2Pos)
  val x1 = p1Pos(0).toDouble
  val x2 = p2Pos(0).toDouble
  val y1 = p1Pos(1).toDouble
  val y2 = p2Pos(1).toDouble
  math.sqrt(math.pow(x1 - x2 , 2  ) + math.pow(y1 - y2 , 2  ))
}


object tsp_bab {
  def main(args: Array[String]) {

    val conf = new SparkConf().setAppName("TSP Application")
    val sc = new SparkContext(conf)


    val solved = solveTspBab(linksData)


    val path = "../TSPSpark/src/TestData/wi29.tsp.txt"
    val nodes = readTspFile(sc, path, 7)
    val edges = mkConnections(sc, nodes)
    val edgesWithDistance = edges.map(x=> (x._1,x._2, distance(x)))


    val linksData = edgesWithDistance.collect
    val solvedFileTodo = solveTspBab(edgesWithDistance)
    val solvedFile = solveTspBab(edgesWithDistance.collect)

    countPath(solvedFile._1, edgesWithDistance.collect)

     //linksData.find(f=> f._1 == x && f._2 == y).get.
     //linksRDD.filter(f=> f._1 == "a"  && f._2 == "b").first._3




  }
}
