package pythian.nifi.processors

import akka.actor.{ Actor, ActorRef, ActorSystem, Props, Terminated }
import akka.pattern.ask
import akka.util.Timeout

import scala.concurrent.duration._
import scala.concurrent.Await

object RoutingSink {

  implicit val timeout: Timeout = 10.second

  trait Sink[KV, Out] {
    def add(elem: KV): Unit
    def close(): Out
  }

  def create[KV, K, Out](init: K => Sink[KV, Out], groupBy: KV => K)(implicit system: ActorSystem): Sink[KV, List[(K, Out)]] = {

    case class Data(kv: KV)
    object Finish
    case class Complete(kOuts: List[(K, Out)])
    case class WorkerComplete(kOut: (K, Out))

    class Router extends Actor {
      private var groups = Map.empty[K, ActorRef]
      private var actors = Map.empty[ActorRef, K]
      private var finishSender: ActorRef = _
      private var kOuts = List.empty[(K, Out)]
      def receive: Receive = {
        case data @ Data(kv) =>
          val k = groupBy(kv)
          val worker = groups.get(k) match {
            case Some(res) =>
              res
            case None =>
              val worker = context.actorOf(Props(new Worker(k)))
              context.watch(worker)
              groups = groups + (k -> worker)
              actors = actors + (worker -> k)
              worker
          }
          worker ! data
        case Finish =>
          finishSender = sender()
          actors.keys.foreach(_ ! Finish)
        case WorkerComplete(kOut) =>
          kOuts = kOut :: kOuts
        case Terminated(actor) =>
          val s = actors(actor)
          groups = groups - s
          actors = actors - actor
          if (actors.isEmpty) {
            finishSender ! Complete(kOuts)
            context.stop(self)
          }
      }
    }

    class Worker(k: K) extends Actor {
      private val sink = init(k)
      def receive: Receive = {
        case Data(kv) =>
          sink.add(kv)
        case Finish =>
          val out = sink.close()
          sender() ! WorkerComplete((k, out))
          context.stop(self)
      }
    }

    lazy val router = system.actorOf(Props(new Router))

    new Sink[KV, List[(K, Out)]] {
      def add(kv: KV): Unit = router ! Data(kv)
      def close(): List[(K, Out)] = Await.result((router ? Finish).mapTo[Complete], timeout.duration).kOuts
    }
  }
}
