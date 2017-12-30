package pythian.nifi.processors

import akka.actor.{ Actor, ActorRef, ActorSystem, Props, Terminated }
import akka.pattern.ask
import akka.util.Timeout

import scala.concurrent.duration._
import scala.concurrent.Await

object RoutingSink {

  implicit val timeout: Timeout = 10.second

  trait Sink[T, R] {
    def add(elem: T): Unit
    def close(): R
  }

  def create[T, S, R](init: S => Sink[T, R], groupBy: T => S)(implicit system: ActorSystem): Sink[T, List[(S, R)]] = {

    case class Data(content: T)
    object Finish
    case class Complete(ret: List[(S, R)])
    case class WorkerComplete(ret: (S, R))

    class Router extends Actor {
      private var groups = Map.empty[S, ActorRef]
      private var actors = Map.empty[ActorRef, S]
      private var finishSender: ActorRef = _
      private var rets = List.empty[(S, R)]
      def receive: Receive = {
        case data @ Data(t) =>
          val s = groupBy(t)
          val worker = groups.get(s) match {
            case Some(res) =>
              res
            case None =>
              val worker = context.actorOf(Props(new Worker(s)))
              context.watch(worker)
              groups = groups + (s -> worker)
              actors = actors + (worker -> s)
              worker
          }
          worker ! data
        case Finish =>
          finishSender = sender()
          actors.keys.foreach(_ ! Finish)
        case WorkerComplete(ret) =>
          rets = ret :: rets
        case Terminated(actor) =>
          val s = actors(actor)
          groups = groups - s
          actors = actors - actor
          if (actors.isEmpty) {
            finishSender ! Complete(rets)
            context.stop(self)
          }
      }
    }

    class Worker(s: S) extends Actor {
      private val sink = init(s)
      def receive: Receive = {
        case Data(t) =>
          sink.add(t)
        case Finish =>
          val ret = sink.close()
          sender() ! WorkerComplete((s, ret))
          context.stop(self)
      }
    }

    lazy val router = system.actorOf(Props(new Router))

    new Sink[T, List[(S, R)]] {
      def add(elem: T): Unit = router ! Data(elem)
      def close(): List[(S, R)] = Await.result((router ? Finish).mapTo[Complete], timeout.duration).ret
    }
  }
}
