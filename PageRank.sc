/* PageRank algorithm with Apache Spark
* Execution:
* Export input into hdfs running: hdfs -put graph.in .
* Run spark-shell
* Run :load pr.sc
* Execute using
* PR.main(Array("InputFile", "MinDeltaIteration"))
* Ex: PR.main(Array("NotreDame.in", "0.1"))
*/

import org.apache.hadoop.fs.FileSystem
import org.apache.hadoop.fs.Path
object PR {
  def main(args: Array[String]) {
    val fs=FileSystem.get(sc.hadoopConfiguration)
    println("Input file: " + args(0))
    var x = args(1).toFloat + 1 -1
    println("Min Delta: " + x)
    val edgesFile = sc.textFile(args(0))

    val pre_original = edgesFile.map(line => line.split("\\s+")).
                          flatMap(e => List((e(0), List(e(1))), (e(1), List()))).
                          reduceByKey((x,y) =>  x++y)

    val nodes = pre_original.count()

    val original = pre_original.map({case (x,y) => (x, (1.0/nodes, y))})

    var last = original
    var i = 0
    val d = 0.85
    val minDelta = args(1).toFloat
    var deltaPR = 1.0
    while(deltaPR > minDelta){
      val emptySum = last.filter({case (_, (pr, neigh)) => neigh.size == 0}).
                      map({case (_, (pr, _)) => pr}).
                      fold(0.0)(_+_)
      val newPR = last.flatMap({case (nid, (pr, neigh)) => neigh.map(x => (x, pr/neigh.length))}).
                    reduceByKey(_+_).
                    map({case (nid, pr) => (nid, (1-d)/nodes + d*pr + d*emptySum/nodes)})
      deltaPR = newPR.union(last.map({case (nid, (pr, _)) => (nid, pr)})).
                reduceByKey({case (x,y) => math.abs(x - y)}).
                map(_._2).
                fold(0.0)(_ + _)
      val next = newPR.join(last.map({case (nid, (_, neigh)) => (nid, neigh)}))
      last = next
      i += 1
      println("DeltaPR after iteration "+i+" is "+deltaPR)
    }

    val endOutputPath=args(0) + ".out"
    if(fs.exists(new Path(endOutputPath)))
      fs.delete(new Path(endOutputPath),true)
    last.saveAsTextFile(endOutputPath)
  }
}
