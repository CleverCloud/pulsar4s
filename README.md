pulsar4s - Apache Pulsar Scala Client
==================================================

[![Build Status](https://travis-ci.org/sksamuel/pulsar4s.png?branch=master)](https://travis-ci.org/sksamuel/pulsar4s)
[<img src="https://img.shields.io/maven-central/v/com.sksamuel.pulsar4s/pulsar4s-core_2.13.svg?label=latest%20release%20for%202.13"/>](http://search.maven.org/#search%7Cga%7C1%7Ca%3A%22pulsar4s-core_2.13%22)
[<img src="https://img.shields.io/maven-central/v/com.sksamuel.pulsar4s/pulsar4s-core_2.12.svg?label=latest%20release%20for%202.12"/>](http://search.maven.org/#search%7Cga%7C1%7Ca%3A%22pulsar4s-core_2.12%22)
[<img src="https://img.shields.io/maven-central/v/com.sksamuel.pulsar4s/pulsar4s-core_2.11.svg?label=latest%20release%20for%202.11"/>](http://search.maven.org/#search%7Cga%7C1%7Ca%3A%22pulsar4s-core_2.11%22)

pulsar4s is a concise, idiomatic, reactive, type safe Scala client for [Apache Pulsar](https://pulsar.apache.org/).
As a simple wrapper over the Java client, we beneift from the reliability and performance of that client while providing better integration with the Scala ecosystem and idioms.

* Supports different effects - [scala.concurrent.Future](https://docs.scala-lang.org/overviews/core/futures.html),
[monix.eval.Task](https://monix.io/docs/2x/eval/task.html),
[cats.effect.IO](https://typelevel.org/blog/2017/05/02/io-monad-for-cats.html),
[scalaz.concurrent.Task](https://github.com/indyscala/scalaz-task-intro/blob/master/presentation.md)
* Uses scala.concurrent.duration.Duration
* Provides case classes rather than Java beans
* [Akka Streams](https://github.com/sksamuel/pulsar4s#akka-streams) source and sink
* Circe, SprayJson, PlayJson and Jackson implementations of Schema typeclass

## Using the client

The first step is to create a client attached to the pulsar cluster, providing the service url.

```scala
val client = PulsarClient("pulsar://localhost:6650")
```

Alternatively, you can use an instance of `PulsarClientConfig` if you need to set further configuration
options such as authentication, tls, timeouts and so on.

```scala
val config = PulsarClientConfig("pulsar://localhost:6650", ...)
val client = PulsarClient(config)
```

Then we can create either a producer or a consumer from the client. We need an implicit schema in scope - more on that later.

To create a producer, we need the topic, and an instance of `ProducerConfig`.
We can set further options on the config object, such as max pending messages, router mode, producer name and so on.

```scala
implicit val schema: Schema[String] = Schema.STRING

val topic = Topic("persistent://sample/standalone/ns1/b")
val producerConfig = ProducerConfig(topic, ...)
val producer = client.producer[String](producerConfig)
```

To create a consumer, we need one or more topics to subscribe to, the subscription name, and an instance of `ConsumerConfig`.
We can set further options on the config object, such as subscription type, consumer name, queue size and so on.

```scala
implicit val schema: Schema[String] = Schema.STRING

val topic = Topic("persistent://sample/standalone/ns1/b")
val consumerConfig = ConsumerConfig(Seq(topic), Subscription("mysub"), ...)
val consumerFn = client.consumer[String](ConsumerConfig(, )
```

Note: Call `close()` on the client, producer, and consumer once you are finished. The client and producer also implement `AutoCloseable` and `Closeable`.

### Schemas

A message must be the correct type for the producer or consumer. When a producer or consumer is created,
an implicit `Schema` typeclass must be available. In the earlier examples, you saw that we added an implicit schema for String using `implicit val schema: Schema[String] = Schema.STRING`.

There are built in schemas for bytes and strings, but other complex types required a custom schema.
Some people prefer to write custom typeclasses manually for the types they need to support.
Other people like to just have it done automagically. For those people, pulsar4s provides extensions
for the well known Scala Json libraries that can be used to generate messages where the body
is a JSON representation of the class.

An example of creating a producer for a complex type using the circe json library to generate the schema:

```scala
import io.circe.generic.auto._
import com.sksamuel.pulsar4s.circe._

val topic = Topic("persistent://sample/standalone/ns1/b")
val producer = client.producer[Food](ProducerConfig(topic))
producer.send(Food("pizza", "ham and pineapple"))
```

Note: The imports bring into scope a method that will generate an implicit schema when required.

The following extension modules can be used for automatic schemas

| Library | Module | Import |
|---------|------------------|--------|
|[Circe](https://github.com/travisbrown/circe)|[pulsar4s-circe](http://search.maven.org/#search%7Cga%7C1%7Cpulsar4s-circe)|import io.circe.generic.auto._ <br/>import com.sksamuel.pulsar4s.circe._|
|[Jackson](https://github.com/FasterXML/jackson-module-scala)|[pulsar4s-jackson](http://search.maven.org/#search%7Cga%7C1%7Cpulsar4s-jackson)|import com.sksamuel.pulsar4s.jackson._|
|Json4s|[pulsar4s-json4s](http://search.maven.org/#search%7Cga%7C1%7Cpulsar4s-json4s)|import com.sksamuel.pulsar4s.json4s._|
|Spray Json|[pulsar4s-spray-json](http://search.maven.org/#search%7Cga%7C1%7Cpulsar4s-spray-json)|import com.sksamuel.pulsar4s.sprayjson._|
|Play Json|[pulsar4s-play-json](http://search.maven.org/#search%7Cga%7C1%7Cpulsar4s-play-json)|import com.sksamuel.pulsar4s.playjson._|


### Producing

There are two ways to send a message - either with a plain value, or with an instance of `ProducerMessage`.
If you do not need to specify extra options on the message - such as key, event time, headers, etc - then you can just send
a plain value, and the client will wrap the value in a pulsar message. Alternatively, you can create an instance of `ProducerMessage`
to specify extra options.

Each method can be synchronous or asynchronous. The asynchronous methods return a `scala.concurrent.Future`.
If you are using another effect library, such as cats, scalaz or monix, then pulsar4s
also supports those effects. See the section on #effects.

If the send method is successful, you will receive the `MessageId` of the generated message. If an exception is generated, then in the synchronous methods, you will receive a `Failure` with the error. In the asynchronous
methods the exception will be surfaced as a failed Future.

To send a plain value, we just invoke `send` with the value:

```scala
producer.send("wibble")
```

Or to send a message, we first create an instance of `ProducerMessage`.

```scala
val message = DefaultProducerMessage(Some("mykey"), "wibble", eventTime = Some(EventTime(System.currentTimeMillis)))
producer.send(message)
```

### Consuming

To recieve a message, create a consumer and invoke either the `receive`, `receive(Duration)`, or the `receiveAsync` methods.
The first two are synchronous and return an instance of `ConsumerMessage`, blocking if necessary, and the latter is asynchronous, returning
a Future (or other effect) with the `ConsumerMessage` once ready.

```scala
val message: Message = consumer.receive
```

or

```scala
val message: Future[T] = consumer.receiveAsync
```

Once a message has been consumed, it is important to acknowledge the message by using the message id with the ack methods.

```scala
consumer.acknowledge(message.messageId)
```


## Akka Streams

Pulsar4s integrates with the outstanding [akka-streams](https://doc.akka.io/docs/akka/2.5.5/scala/stream/index.html) library - it provides both a source and a sink.
To use this, you need to add a dependency on the `pulsar4s-akka-streams` module.

### Sources

To create a source all that is required is a function that will create a consumer on demand and the message id to seek.
The function must return a fresh consumer each time it is invoked.
The consumer is just a regular pulsar4s `Consumer` and can be created in the normal way, for example.

```scala
val topic = Topic("persistent://sample/standalone/ns1/b")
val consumerFn = () => client.consumer(ConsumerConfig(topic, subscription))
```

We pass that function into the source method, providing the seek. Note the imports.

```scala
import com.sksamuel.pulsar4s.akka.streams._
val pulsarSource = source(consumerFn, Some(MessageId.earliest))
```

The materialized value of the source is an instance of `Control` which provides a method called 'close' which can be used to stop consuming messages.
Once the akka streams source is completed (or fails) the consumer will be automatically closed.

### Sinks

To create a sink, we need a producer function similar to the source's consumer function.
Again, the producer used is just a regular pulsar4s `Producer`.
The function must return a fresh producer each time it is invoked.

```scala
val topic = Topic("persistent://sample/standalone/ns1/b")
val producerFn = () => client.producer(ProducerConfig(topic))
```

We pass that function into the sink method. Once again, take note of the imports.

```scala
import com.sksamuel.pulsar4s.akka.streams._
val pulsarSink = sink(producerFn)
```

A sink requires messages of type `ProducerMessage[T]` where T is the value type of the message. For example, if we were producing
String messages, then we would map our upstream messages into instances of `ProducerMessage[String]` before passing them to the sink.

```scala
import com.sksamuel.pulsar4s.akka.streams._
Source.fromIterator(() => List("a", "b", "c", "d").iterator)
  .map(string => ProducerMessage(string))
  .runWith(sink(producerFn))
```

A sink will run until the upstream source completes. In other words, to terminate the sink, the source must be cancelled or completed.
Once the sink completes the producer will be automatically closed.

The materialized value of the sink is a `Future[Done]` which will be completed once the upstream source has completed.


### Full Example

Here is a full example of consuming from a topic for 10 seconds, publising the messages back into another topic.
Obviously this is a bit of a toy example but shows everything in one place.

```scala
import com.sksamuel.pulsar4s.{ConsumerConfig, MessageId, ProducerConfig, PulsarClient, Subscription, Topic}
import org.apache.pulsar.client.api.Schema

implicit val system: ActorSystem = ActorSystem()
implicit val materializer: ActorMaterializer = ActorMaterializer()
implicit val schema: Schema[Array[Byte]] = Schema.BYTES

val client = PulsarClient("pulsar://localhost:6650")

val intopic = Topic("persistent://sample/standalone/ns1/in")
val outtopic = Topic("persistent://sample/standalone/ns1/out")

val consumerFn = () => client.consumer(ConsumerConfig(Seq(intopic), Subscription("mysub")))
val producerFn = () => client.producer(ProducerConfig(outtopic))

val control = source(consumerFn, Some(MessageId.earliest))
                .map { consumerMessage => ProducerMessage(consumerMessage.data) }
                .to(sink(producerFn)).run()

Thread.sleep(10000)
control.close()
```

## Example SBT Setup

```scala
val pulsar4sVersion = "x.x.x"
libraryDependencies ++= Seq(
  "com.sksamuel.pulsar4s" %% "pulsar4s-core" % pulsar4sVersion,

  // for the akka-streams integration
  "com.sksamuel.pulsar4s" %% "pulsar4s-akka-streams" % pulsar4sVersion,

  // if you want to use circe for schemas
  "com.sksamuel.pulsar4s" %% "pulsar4s-circe" % pulsar4sVersion,

  // if you want to use json4s for schemas
  "com.sksamuel.pulsar4s" %% "pulsar4s-json4s" % pulsar4sVersion,

  // if you want to use jackson for schemas
  "com.sksamuel.pulsar4s" %% "pulsar4s-jackson" % pulsar4sVersion,

  // if you want to use spray-json for schemas
  "com.sksamuel.pulsar4s" %% "pulsar4s-spray-json" % pulsar4sVersion,

  // if you want to use play-json for schemas
  "com.sksamuel.pulsar4s" %% "pulsar4s-play-json" % pulsar4sVersion,

  // if you want to use monix effects
  "com.sksamuel.pulsar4s" %% "pulsar4s-monix" % pulsar4sVersion,

  // if you want to use scalaz effects
  "com.sksamuel.pulsar4s" %% "pulsar4s-scalaz" % pulsar4sVersion,

  // if you want to use cats effects
  "com.sksamuel.pulsar4s" %% "pulsar4s-cats-effect" % pulsar4sVersion,
)
```

## Contributions
Contributions to pulsar4s are always welcome. Good ways to contribute include:

* Raising bugs and feature requests
* Improving the performance of pulsar4s
* Adding to the documentation

## License
```
This software is licensed under the Apache 2 license, quoted below.

Copyright 2017-2018 Stephen Samuel

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
