package xyz.sourcecodestudy.rpc.akka

import akka.actor.typed.scaladsl.Behaviors
import akka.actor.typed.SpawnProtocol
import akka.actor.typed.{ ActorRef, ActorSystem, Behavior }

// From: https://doc.akka.io/docs/akka/2.6/typed/actors.html

final case class Greet(whom: String, replyTo: ActorRef[Greeted])
final case class Greeted(whom: String, from: ActorRef[Greet])

object HelloWorld {

  def apply(): Behavior[Greet] = Behaviors.receive { (context, message) =>
    context.log.info("Hello {}!", message.whom)
    message.replyTo ! Greeted(message.whom, context.self)
    Behaviors.same
  }
}

object HelloWorldBot {

  def apply(max: Int): Behavior[Greeted] = {
    bot(0, max)
  }

  private def bot(greetingCounter: Int, max: Int): Behavior[Greeted] =
    Behaviors.receive { (context, message) =>
      val n = greetingCounter + 1
      context.log.info("Greeting {} for {}", n, message.whom)
      if (n == max) {
        Behaviors.stopped
      } else {
        message.from ! Greet(message.whom, context.self)
        bot(n, max)
      }
    }
}

object HelloWorldMain {

  def apply(message: String): Behavior[SpawnProtocol.Command] = {
    Behaviors.setup { context =>

      val greeter = context.spawn(HelloWorld(), "greeter")
      val replyTo = context.spawn(HelloWorldBot(max = 3), "replyer")

      greeter ! Greet(message, replyTo)

      SpawnProtocol()
    }
  }

  def main(args: Array[String]): Unit = {

    val message = args.toSeq match {
      case head :: tail => head
      case Nil    => "World"
    }

    // Init
    ActorSystem(HelloWorldMain(message), "hello")
  }
}