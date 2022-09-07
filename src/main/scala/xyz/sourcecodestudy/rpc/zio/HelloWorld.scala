package xyz.sourcecodestudy.rpc.zio

import zio.actors.Actor.Stateful
import zio.actors._
import zio.clock.Clock
import zio.console.putStrLn
import zio.{ExitCode, UIO, URIO, ZIO}

sealed trait Message[+_] 
case object Increase extends Message[Unit]
case object Get      extends Message[Int]

object CounterActorExample extends zio.App {

  val counterActor: Stateful[Any, Int, Message] = {
    new Stateful[Any, Int, Message] {
      override def receive[A](state: Int, msg: Message[A], context: Context): UIO[(Int, A)] = {
        msg match {
          case Increase => UIO((state + 1, ()))
          case Get      => UIO((state, state))
        }
      }
    }
  }

  val myApp: ZIO[Clock, Throwable, Int] = {
    for {
      system <- ActorSystem("MyActorSystem")
      actor  <- system.make("counter", Supervisor.none, 0, counterActor)
      _      <- actor ! Increase
      _      <- actor ! Increase
      s      <- actor ? Get
    } yield s
  }

  override def run(args: List[String]): URIO[zio.ZEnv, ExitCode] = {
    myApp
      .flatMap { state =>
        putStrLn(s"The final state of counter: ${state}")
      }
      .exitCode
  }
}