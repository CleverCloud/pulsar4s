pulsar4s - Apache Pulsar Scala Client
==================================================

[![Build Status](https://travis-ci.org/sksamuel/pulsar4s.png?branch=master)](https://travis-ci.org/sksamuel/pulsar4s)
[<img src="https://img.shields.io/maven-central/v/com.sksamuel.pulsar4s/pulsar4s-core_2.11.svg?label=latest%20release%20for%202.11"/>](http://search.maven.org/#search%7Cga%7C1%7Ca%3A%22pulsar4s-core_2.11%22)
[<img src="https://img.shields.io/maven-central/v/com.sksamuel.pulsar4s/pulsar4s-core_2.12.svg?label=latest%20release%20for%202.12"/>](http://search.maven.org/#search%7Cga%7C1%7Ca%3A%22pulsar4s-core_2.12%22)

pulsar4s is a concise, idiomatic, reactive, type safe Scala client for [Apache Pulsar](https://pulsar.apache.org/).
The official Java client can of course be used, but this client provides better integration with Scala.

* Supports different effects - [scala.concurrent.Future](https://docs.scala-lang.org/overviews/core/futures.html),
[monix.eval.Task](https://monix.io/docs/2x/eval/task.html),
[cats.effect.IO](https://typelevel.org/blog/2017/05/02/io-monad-for-cats.html),
[scalaz.concurrent.Task](https://github.com/indyscala/scalaz-task-intro/blob/master/presentation.md)
* Uses scala.concurrent.duration.Duration
* Provides case classes rather than Java beans
* Reactive Streams implementation for streaming data in and out of Pulsar
* Typeclasses for marshalling to/from Pulsar messages
* Circe and Jackson implementations of said typeclasses

## Using the client

The first step is to create a client attached to the pulsar cluster.

`val client = PulsarClient("pulsar://localhost:6650", "sample/standalone/ns1")`

Then we can create either a producer or a consumer by passing in a topic.

```scala
val topic = Topic("persistent://sample/standalone/ns1/b")
val producer = client.producer(topic)
```

```scala
val topic = Topic("persistent://sample/standalone/ns1/b")
val consumer = client.consumer(topic, Subscription("mysub"))
```

The producer and consumer methods also accept a configuration argument. Note that the consumer requires a `subscription` argument.

Note: Call `close()` on the client, producer, and consumer once you are finished.

### Sending

To send a message, take a producer and invoke either the `send` method, which is synchronous, or the `sendAsync` method which is asynchronous. The methods
will return the `MessageId` of the message produced. For example:

```scala
val messageId: MessageId = producer.send("wibble")
```

or

```scala
val messageId: Future[MessageId] = producer.sendAsync("wibble")
```

Note that the async method returns a `scala.concurrent.Future`. If you are using another effect library, such as cats or monix, then pulsar4s
also supports those effects. See the section on #effects.

If an exception is generated, then in the synchronous methods, the exception will simply be thrown. In the asynchronous
methods the exception will be surfaced as a failed Future.

If you prefer to have explicit error handling, then you can use the `trySend` methods which, instead of
throwing exceptions, will return a `Try[MessageId]`. The asynchronous methods don't need this of course, as the error
handling is already present as the failed state.

### Receiving

To recieve a message, take a consumer and invoke either the `receive`, `receive(Duration)`, or the `receiveAsync` methods.
The first two are synchronous and the latter is asynchronous.

```scala
val message: Message = consumer.receive
```

or

```scala
val message: Future[Message] = producer.receiveAsync
```

Error handling is the same as for sending, with the methods called `tryReceive`.


## Marshalling to/from classes

Sometimes it is useful to send / receive messages directly using classes from your domain model.
For this, pulsar4s provides the `MessageWriter` and `MessageReader` typeclasses, which are used to generate
pulsar `Message`s from ordinary classes.

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

## Contributions
Contributions to pulsar4s are always welcome. Good ways to contribute include:

* Raising bugs and feature requests
* Fixing bugs and enhancing the DSL
* Improving the performance of elastic4s
* Adding to the documentation

## License
```
This software is licensed under the Apache 2 license, quoted below.

Copyright 2018 Stephen Samuel

Licensed under the Apache License, Version 2.0 (the "License"); you may not
use this file except in compliance with the License. You may obtain a copy of
the License at

    http://www.apache.org/licenses/LICENSE-2.0

Unless required by applicable law or agreed to in writing, software
distributed under the License is distributed on an "AS IS" BASIS, WITHOUT
WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied. See the
License for the specific language governing permissions and limitations under
the License.
```