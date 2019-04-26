package org.ditw.sparkRuns
import akka.{Done, NotUsed}
import akka.actor.ActorSystem
import akka.stream.ActorMaterializer
import akka.stream.scaladsl.{Flow, Sink, Source}

import scala.concurrent.Future

object AkkaStrmTest extends App {
  import scala.concurrent.ExecutionContext.Implicits.global

  implicit val sys = ActorSystem("sys")
  implicit val mat = ActorMaterializer()
  val nums = 1 to 100

  val numSrc:Source[Int, NotUsed] = Source.fromIterator(() => nums.iterator)

  val isEvenFlow:Flow[Int, Int, NotUsed] = Flow[Int].filter(_ % 2 == 0)

  val evenNumSrc = numSrc.via(isEvenFlow)

  val consoleSink:Sink[Int, Future[Done]] = Sink.foreach[Int](println)

  val done:Future[Done] = evenNumSrc.runWith(consoleSink)

  done.onComplete(_ => sys.terminate())

}
