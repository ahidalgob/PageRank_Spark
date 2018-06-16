import org.apache.hadoop.fs.FileSystem
import org.apache.hadoop.fs.Path
val fs=FileSystem.get(sc.hadoopConfiguration)

val edgesFile = sc.textFile("SmallGraph.txt")

val pre_original = edgesFile.map(line => line.split("\\s+")).
                          flatMap(e => List((e(0), List(e(1))), (e(1), List()))).
                          reduceByKey((x,y) =>  x++y)

val nodes = pre_original.count()

val original = pre_original.map({case (x,y) => (x, (1.0/nodes, y))})

val originalOutputPath="original"
if(fs.exists(new Path(originalOutputPath)))
  fs.delete(new Path(originalOutputPath),true)
original.saveAsTextFile(originalOutputPath)


var last = original
var i = 0
val d = 0.85
val minDelta = 0.01
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

val endOutputPath="SmallGraph.out"
if(fs.exists(new Path(endOutputPath)))
  fs.delete(new Path(endOutputPath),true)
last.saveAsTextFile(endOutputPath)
