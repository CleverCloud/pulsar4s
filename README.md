# pulsar4s

pulsar4s - Apache Pulsar Scala Client
==================================================

[![Build Status](https://travis-ci.org/sksamuel/pulsar4s.png?branch=master)](https://travis-ci.org/sksamuel/pulsar4s)
[<img src="https://img.shields.io/maven-central/v/com.sksamuel.pulsar4s/pulsar4s-core_2.11.svg?label=latest%20release%20for%202.11"/>](http://search.maven.org/#search%7Cga%7C1%7Ca%3A%22pulsar4s-core_2.11%22)
[<img src="https://img.shields.io/maven-central/v/com.sksamuel.pulsar4s/pulsar4s-core_2.12.svg?label=latest%20release%20for%202.12"/>](http://search.maven.org/#search%7Cga%7C1%7Ca%3A%22pulsar4s-core_2.12%22)

pulsar4s is a concise, idiomatic, reactive, type safe Scala client for Apache Pulsar.
The official Java client can of course be used in Scala, but this client provides better integration with Scala.

* Supports scala.Future, monix.Task, cats.Effect, scalaz.Task
* Uses scala.concurrent.duration.Duration
* Provides case classes rather than Java beans
* Reactive Streams implementation
* Typeclasses for marshalling to/from Pulsar messages
* Circe and Jackson implementations of said typeclasses