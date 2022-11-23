# Spark's RPC 
> The implement with ZIO is [here](https://github.com/changzhiwin/zio-actor). It's more easier than this one.

Spark drop [akka](https://doc.akka.io/docs/akka/2.6/typed/actors.html#first-example) dependence sence 2.0, and rewrite self rpc module(basic on Netty). But it copy a lot concept from Akka Actor. This project take only rpc part from Spark [source code](https://github.com/changzhiwin/spark-core-analysis), and make it run independently.
## Framework of RPC
![rpc-framework](./doc/img/rpc-framework.png)


## RPC VS Akka Actor VS ZIO Actor
> zio actor is [here](https://github.com/changzhiwin/zio-actor)

I write simple code to compare both of them. It's very similar, some concept mapping like this:
- RpcEndpoint     -> Actor
- RpcEndpointRef  -> ActorRef
- RpcEnv          -> ActorSystem

You will better understand it with below program.

### Environment
- Java 1.8
- Scala 2.13.8

### How to run
```
1, open shell terminal at this project root dir

2, $> sbt

3, $> runMain xyz.sourcecodestudy.rpc.example.HelloWorldMain

4, $> runMain xyz.sourcecodestudy.rpc.akka.HelloWorldMain

// All details of console output
changzhi@changzhi spark-rpc % sbt
[info] welcome to sbt 1.6.2 (Oracle Corporation Java 1.8.0_191)
[info] loading project definition from /Users/changzhi/Scala/github/spark-rpc/project
[info] loading settings for project spark-rpc from build.sbt ...
[info] set current project to spark-rpc (in build file:/Users/changzhi/Scala/github/spark-rpc/)
[info] sbt server started at local:///Users/changzhi/.sbt/1.0/server/96df759d83040ce5c3db/sock
[info] started sbt server
sbt:spark-rpc> runMain xyz.sourcecodestudy.rpc.example.HelloWorldMain
[info] running xyz.sourcecodestudy.rpc.example.HelloWorldMain 
2022-09-04 10:16:11 INFO HelloWorld: Hello World!
2022-09-04 10:16:11 INFO HelloWorldBot: Greeting 1 for World
2022-09-04 10:16:11 INFO HelloWorld: Hello World!
2022-09-04 10:16:11 INFO HelloWorldBot: Greeting 2 for World
2022-09-04 10:16:11 INFO HelloWorld: Hello World!
2022-09-04 10:16:11 INFO HelloWorldBot: Greeting 3 for World
[success] Total time: 7 s, completed 2022-9-4 10:16:11
sbt:spark-rpc> runMain xyz.sourcecodestudy.rpc.akka.HelloWorldMain
[info] running xyz.sourcecodestudy.rpc.akka.HelloWorldMain 
SLF4J: akka.event.slf4j.Slf4jLogger
SLF4J: The following set of substitute loggers may have been accessed
SLF4J: during the initialization phase. Logging calls during this
SLF4J: phase were not honored. However, subsequent logging calls to these
SLF4J: loggers will work as normally expected.
SLF4J: See also http://www.slf4j.org/codes.html#substituteLogger
[success] Total time: 2 s, completed 2022-9-4 10:16:22
2022-09-04 10:16:22 INFO HelloWorld$: Hello World!
2022-09-04 10:16:22 INFO HelloWorldBot$: Greeting 1 for World
2022-09-04 10:16:22 INFO HelloWorld$: Hello World!
2022-09-04 10:16:22 INFO HelloWorldBot$: Greeting 2 for World
2022-09-04 10:16:22 INFO HelloWorld$: Hello World!
2022-09-04 10:16:22 INFO HelloWorldBot$: Greeting 3 for World
sbt:spark-rpc> exit
[info] shutting down sbt server

```

### Use Spark Rpc
```
// xyz.sourcecodestudy.rpc.example.HelloWorld.scala

case class Greet(whom: String, replyTo: RpcEndpointRef)
case class Greeted(whom: String, from: RpcEndpointRef)

class HelloWorld(override val rpcEnv: RpcEnv) extends RpcEndpoint with Logging {

  override def receive: PartialFunction[Any, Unit] = {
    case Greet(whom, replyTo) => {

      logger.info(s"Hello ${whom}!")

      replyTo.send(Greeted(whom, self))
    }
  }

}

class HelloWorldBot(override val rpcEnv: RpcEnv, max: Int) extends RpcEndpoint with Logging {

  private var replyTimes = 0

  override def receive: PartialFunction[Any, Unit] = {
    case Greeted(whom, from) => {

      replyTimes += 1
      logger.info(s"Greeting ${replyTimes} for ${whom}")

      if (replyTimes < max) {
        from.send(Greet(whom, self))
      } else {
        rpcEnv.shutdown()
      }
    }
  }
}

object HelloWorldMain {

  def apply(message: String): Unit = {
    
    val settings = new RpcSettings()
    val rpcEnv = RpcEnv.create("hello", "127.0.0.1", 9999, 1, settings)

    val greeter = rpcEnv.setupEndpoint("greeter", new HelloWorld(rpcEnv))
    val replyTo = rpcEnv.setupEndpoint("replyer", new HelloWorldBot(rpcEnv, max = 3))

    greeter.send( Greet(message, replyTo) )

    rpcEnv.awaitTermination()
  }

  def main(args: Array[String]): Unit = {

    val message = args.toSeq match {
      case head :: tail => head
      case Nil    => "World"
    }

    HelloWorldMain(message)
  }
}
```

### Use Akka Actor
```
// xyz.sourcecodestudy.rpc.akka.HelloWorld

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

```