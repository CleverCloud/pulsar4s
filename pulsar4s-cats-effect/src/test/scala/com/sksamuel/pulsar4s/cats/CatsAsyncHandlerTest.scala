package com.sksamuel.pulsar4s.cats

import java.util.UUID

import com.sksamuel.pulsar4s._
import org.apache.pulsar.client.api.Schema
import org.scalatest.BeforeAndAfterAll
import _root_.cats.effect._
import _root_.cats._
import _root_.cats.data._
import _root_.cats.implicits._
import org.scalatest.funsuite.AnyFunSuite
import org.scalatest.matchers.should.Matchers

class CatsAsyncHandlerTest extends AnyFunSuite with Matchers with BeforeAndAfterAll {


  implicit val schema: Schema[String] = Schema.STRING

  val client = PulsarClient("pulsar://localhost:6650")
  val topic = Topic("persistent://sample/standalone/ns1/cats_" + UUID.randomUUID())

  override def afterAll(): Unit = {
    client.close()
  }

  test("async producer should be able to use cats IO with the standard import") {
    import CatsAsyncHandler._
    val producer = client.producer(ProducerConfig(topic))
    val t = producer.sendAsync("wibble")
    t.unsafeRunSync() should not be null
    producer.close()
  }

  test("async consumer should be able to use cats IO with the standard import") {
    import CatsAsyncHandler._
    val consumer = client.consumer(ConsumerConfig(topics = Seq(topic), subscriptionName = Subscription("mysub_" + UUID.randomUUID)))
    consumer.seekEarliest()
    val t = consumer.receiveAsync
    new String(t.unsafeRunSync().data) shouldBe "wibble"
    consumer.close()
  }

  def pulsarResources[F[_]: Sync: AsyncHandler](c: PulsarClient, t: Topic, subscription: Subscription): Resource[F, (Producer[String], Consumer[String])] = {
    val producer: Resource[F, Producer[String]] = Resource.make(Sync[F].delay { c.producer(ProducerConfig(t)) }){_.closeAsync}
    val consumer: Resource[F, Consumer[String]] = Resource.make(Sync[F].delay { c.consumer(ConsumerConfig(topics = Seq(t), subscriptionName = subscription)) }){_.closeAsync}
    for (p <- producer; c <- consumer) yield (p, c)
  }

  def asyncProgram[F[_]: Async: AsyncHandler](producer: Producer[String], consumer: Consumer[String], message: String): F[ConsumerMessage[String]] = for {
    _      <- producer.sendAsync(message)
    result <- consumer.receiveAsync
  } yield result

  test("async client methods should work with any monad which implements Async - IO") {
    import CatsAsyncHandler._
    val msg = "hello cats-effect IO"
    pulsarResources[IO](
      client,
      Topic("persistent://sample/standalone/ns1/cats_async_io"),
      Subscription("cats_effect_test_IO")
    ).use { case (producer, consumer) =>
        asyncProgram[IO](producer, consumer, msg)
    }.map(_.value).unsafeRunSync() shouldBe msg
  }

  test("async client methods should work with any monad which implements Async - StateT[IO, Int, ?]") {
    import CatsAsyncHandler._
    type F[T] = StateT[IO, Int, T] // this would be `StateT[IO, Int, ?]` with kind projector
    val msg = "hello cats-effect monad transformers"
    pulsarResources[IO](
      client,
      Topic("persistent://sample/standalone/ns1/cats_async_statet_io"),
      Subscription("cats_effect_test_StateT_IO")
    ).use { case (producer, consumer) =>
      asyncProgram[F](producer, consumer, msg).run(123)
    }.map { case (a, msg) => (a, msg.value) }.unsafeRunSync() shouldBe (123, msg)
  }

  test("async client methods should work with any monad which implements Async - Monix Task") {
    import monix.eval.Task
    import monix.execution.Scheduler.Implicits.global
    import CatsAsyncHandler._
    val msg = "hello monix via cats-effect"
    pulsarResources[Task](
      client,
      Topic("persistent://sample/standalone/ns1/cats_async_monix_task"),
      Subscription("cats_effect_test_monix_task")
    ).use { case (producer, consumer) =>
      asyncProgram[Task](producer, consumer, msg)
    }.map(_.value).runSyncUnsafe() shouldBe msg
  }

  test("async client methods should work with any monad which implements Async - ZIO") {
    import CatsAsyncHandler._
    val msg = "hello ZIO via cats-effect"
    import zio.{Task, Runtime}
    import zio.interop.catz._
    val program = pulsarResources[Task](
      client,
      Topic("persistent://sample/standalone/ns1/cats_async_zio_task"),
      Subscription("cats_effect_test_zio_task")
    ).use { case (producer, consumer) =>
      asyncProgram[Task](producer, consumer, msg)
    }.map(_.value)
    Runtime.default.unsafeRun(program) shouldBe msg
  }
}
