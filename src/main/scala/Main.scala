import akka.NotUsed
import akka.actor.ActorSystem
import akka.stream.alpakka.file.scaladsl.Directory
import akka.stream.scaladsl._

import java.io.File
import java.nio.file.Paths
import scala.concurrent._
import scala.math.Ordered.orderingToOrdered

object Main {
  def main(args: Array[String]): Unit = {

    implicit val system: ActorSystem = ActorSystem("Sensor")

    implicit val ec: ExecutionContextExecutor = system.dispatcher

    val dirName = if (args.length > 0) args(0) else "sensors"

    val files: Source[File, NotUsed] = Directory.ls(Paths.get(dirName))
      .map(p => p.toFile)

    val noOfFiles: RunnableGraph[Future[Int]]  = files.fold(0)((x,_) => x+1).toMat(Sink.last)(Keep.right)

    val rawReadings = files.flatMapConcat(f => Source.fromIterator(() => scala.io.Source.fromFile(f).getLines()))
      .filterNot(_.equals("sensor-id,humidity"))

    val noOfReadings: RunnableGraph[Future[Int]] = rawReadings.fold(0)((x, _) => x + 1).toMat(Sink.last)(Keep.right)

    val failedReadings = rawReadings.filter(_.endsWith("NaN"))

    val noOfFailedReadings: RunnableGraph[Future[Int]] = failedReadings.fold(0)((x, _) => x + 1).toMat(Sink.last)(Keep.right)

    val parsedReadings = rawReadings.map(_.split(',')).map(r => (r(0),r(1).toIntOption))

    val result = parsedReadings
      .groupBy(Int.MaxValue,_._1) //sensor-id,min,avg,max  , sum, count
      .fold[(String,Option[Int], Option[Int], Option[Int], Option[Int], Option[Int])](("", None, None, None, None, None ))((x,y) => { (
        y._1,
       (x._2,y._2) match {
         case (Some(v1), Some(v2)) => if (v1 < v2) Some(v1) else Some(v2)
         case (None,v) => v
         case (v, None) => v
       },
        (x._5, y._2) match {
          case (Some(v1), Some(v2)) => Some((v1+v2) / (x._6.get + 1))
          case (None, v) => v
          case (v, None) => v
        },

        (x._4, y._2) match {
          case (Some(v1), Some(v2)) => if (v1 < v2) Some(v2) else Some(v1)
          case (None, v) => v
          case (v, None) => v
        },
        (x._5, y._2) match {
          case (Some(v1), Some(v2)) =>  Some(v1 + v2)
          case (None, v) => v
          case (v, None) => v
        },
        (x._6, y._2) match {
          case (Some(v1), Some(v2)) => Some(v1+1)
          case (None, Some(v)) => Some(1)
          case (v, None) => v
        },
      )

      })
      .map(x => (x._1, x._2,x._3, x._4))
      .mergeSubstreams

    implicit  object myord extends Ordering[(String, Option[Int], Option[Int], Option[Int])] {
       def compare(a: (String, Option[Int], Option[Int], Option[Int]), b: (String, Option[Int], Option[Int], Option[Int]) ) =
         b._3.compare(a._3)

     }
    val sortedResult = result.runWith(Sink.seq).map(_.toList.sorted)

    def toString(myopt : Option[Int]) = if (myopt.isEmpty) "NaN" else myopt.get.toString

    def show(t: (String, String, String, String)): String = t._1 + "," + t._2 + "," + t._3 + "," + t._4

    val finalResult : Future[Unit] = noOfFiles.run().flatMap(n => {
      println("Num of processed files: " + n)
      noOfReadings.run().flatMap(n => {
        println("Num of processed measurements: " + n)
        noOfFailedReadings.run().flatMap(n => {
          println("Num of failed measurements: " + n)
          println("\nSensors with highest avg humidity: ")
          println("\nsensor-id,min,avg,max: ")
          sortedResult.map(x => {
            x.map(e => show((e._1, toString( e._2), toString(e._3), toString(e._4)))).foreach(println(_))
          })
        })
      })
    })
    finalResult.onComplete(_ => system.terminate())
  }
}