pulsar4s - Apache Pulsar Scala Client
==================================================

[![Build Status](https://travis-ci.org/sksamuel/pulsar4s.png?branch=master)](https://travis-ci.org/sksamuel/pulsar4s)
[<img src="https://img.shields.io/maven-central/v/com.sksamuel.pulsar4s/pulsar4s-core_2.11.svg?label=latest%20release%20for%202.11"/>](http://search.maven.org/#search%7Cga%7C1%7Ca%3A%22pulsar4s-core_2.11%22)
[<img src="https://img.shields.io/maven-central/v/com.sksamuel.pulsar4s/pulsar4s-core_2.12.svg?label=latest%20release%20for%202.12"/>](http://search.maven.org/#search%7Cga%7C1%7Ca%3A%22pulsar4s-core_2.12%22)

pulsar4s is a concise, idiomatic, reactive, type safe Scala client for Apache Pulsar.
The official Java client can of course be used in Scala, but this client provides better integration with Scala.

* Supports different effects - [scala.concurrent.Future](https://docs.scala-lang.org/overviews/core/futures.html),
[monix.eval.Task](https://monix.io/docs/2x/eval/task.html),
[cats.effect.IO](https://typelevel.org/blog/2017/05/02/io-monad-for-cats.html),
[scalaz.concurrent.Task](https://github.com/indyscala/scalaz-task-intro/blob/master/presentation.md)
* Uses scala.concurrent.duration.Duration
* Provides case classes rather than Java beans
* Better type safety
* Reactive Streams implementation for streaming data in and out of Pulsar
* Typeclasses for marshalling to/from Pulsar messages
* Circe and Jackson implementations of said typeclasses


## Marshalling to/from Classes

Sometimes it is useful to send / receive messages directly using classes from your domain model.
For this, pulsar4s provides the `MessageWriter` and `MessageReader` typeclasses.

### Sending

When sending messages simply provide an implicit instance of `MessageWriter[T]` in scope for any class T
that you wish to send a message for, and then use the `producer.send(t)` or `producer.sendAsync(t)` methods.

For example:

```scala
// a simple example of a domain model
case class Person(name: String, location: String)

// how you turn the type into a message is up to you
implicit object PersonWriter extends MessageWriter[Person] {
  override def write(p: Person): Try[Message] = Success(Message(p.name + "/" + p.location))
}

// now the send reads much cleaner
val jon = Person("jon snow", "the wall")
producer.sendAsync(jon)
```

Some people prefer to write typeclasses manually for the types they need to support, as in the example above.
Other people like to just have it done automagically. For those people, pulsar4s provides extensions
for the well known Scala Json libraries that can be used to generate messages where the body
is a JSON representation of the class.

Simply add the import for your chosen library below and then with those implicits in scope,
you can now pass any type you like to the send methods and a MessageWriter will be derived automatically.

| Library | Module | Import |
|---------|------------------|--------|
|[Circe](https://github.com/travisbrown/circe)|[pulsar4s-circe](http://search.maven.org/#search%7Cga%7C1%7Cpulsar4s-circe)|import io.circe.generic.auto._ <br/>import com.sksamuel.pulsar4s.circe._|
|[Jackson](https://github.com/FasterXML/jackson-module-scala)|[pulsar4s-jackson](http://search.maven.org/#search%7Cga%7C1%7Cpulsar4s-jackson)|import com.sksamuel.pulsar4s.jackson.Jackson._|

### Receiving

Just like sending, but in reverse, you can use the `MessageReader` typeclass to derive a type T from
an incoming message. Bring the typeclass into scope, and then use the `receiveT` or `receiveAsyncT`
methods on a consumer.

For example:

```scala
// a simple example of a domain model
case class Person(name: String, location: String)

// how you read the message is up to you
implicit object PersonReader extends MessageReader[Person] {
    override def read(msg: Message): Try[Person] = {
      val str = new String(msg.data)
      str.split('/') match {
        case Array(name, location) => Success(Person(name, location))
        case _ => Failure(new RuntimeException(s"Unable to parse $str"))
      }
    }
}

// now the receive reads much cleaner
val f = producer.receiveAsyncT[Person](jon)
// f contains a success of Person or a failure if it could not be unmarshalled
```
