package com.sksamuel.pulsar4s.cats

import java.util.UUID
import _root_.cats.data._
import _root_.cats.effect._
import _root_.cats.implicits._
import com.sksamuel.pulsar4s._
import org.apache.pulsar.client.api.Schema
import org.scalatest.BeforeAndAfterAll
import org.scalatest.concurrent.Eventually
import org.scalatest.funsuite.AnyFunSuite
import org.scalatest.matchers.should.Matchers

import scala.language.higherKinds
import scala.util.Random

class CatsAsyncHandlerTest extends AnyFunSuite with Matchers with BeforeAndAfterAll with Eventually {

  implicit val schema: Schema[String] = Schema.STRING

  private val client = PulsarClient("pulsar://localhost:6650")
  private val topic = Topic("persistent://sample/standalone/ns1/cats_" + UUID.randomUUID())

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

  test("async consumer getMessageById should be able to use cats IO with the standard import") {
    import CatsAsyncHandler._
    val consumer = client.consumer(ConsumerConfig(topics = Seq(topic), subscriptionName = Subscription("mysub_" + UUID.randomUUID)))
    consumer.seekEarliest()
    val receive = consumer.receiveAsync
    val value = receive.unsafeRunSync()
    val t = consumer.getLastMessageIdAsync
    val r = t.unsafeRunSync()
    r.ledgerId shouldBe value.messageId.ledgerId
    r.entryId shouldBe value.messageId.entryId
    r.partitionIndex shouldBe value.messageId.partitionIndex
    consumer.close()
  }

  def syncResources[F[_] : Async : AsyncHandler](c: PulsarClient, t: Topic, subscription: Subscription): Resource[F, (Producer[String], Consumer[String])] = {
    val producer: Resource[F, Producer[String]] = Resource.make(Sync[F].delay {
      c.producer(ProducerConfig(t))
    }) {
      _.closeAsync
    }
    val consumer: Resource[F, Consumer[String]] = Resource.make(Sync[F].delay {
      c.consumer(ConsumerConfig(topics = Seq(t), subscriptionName = subscription))
    }) {
      _.closeAsync
    }
    for (p <- producer; c <- consumer) yield (p, c)
  }

  def asyncResources[F[_] : Async : AsyncHandler](c: PulsarAsyncClient, t: Topic, subscription: Subscription): Resource[F, (Producer[String], Consumer[String])] = {
    for {
      producer <- Resource.make(c.producerAsync(ProducerConfig(t)))(_.closeAsync)
      consumer <- Resource.make(c.consumerAsync(ConsumerConfig(
        topics = Seq(t),
        subscriptionName = subscription,
      )))(c => c.unsubscribeAsync *> c.closeAsync)
    } yield producer -> consumer
  }

  def asyncProgram[F[_] : Async : AsyncHandler](producer: Producer[String], consumer: Consumer[String], message: String): F[ConsumerMessage[String]] = for {
    _ <- producer.sendAsync(message)
    result <- consumer.receiveAsync
  } yield result

  def asyncBatchProgram[F[_] : Async : AsyncHandler](producer: Producer[String], consumer: Consumer[String], message: String): F[Vector[ConsumerMessage[String]]] = for {
    _ <- (0 to 5).toList.map(id => producer.sendAsync[F](s"${message}_$id")).sequence
    result <- consumer.receiveBatchAsync
  } yield result

  test("async consumer/producer/reader should be lazy") {
    import CatsAsyncHandler._
    val topic = Topic("persistent://sample/standalone/ns1/" + UUID.randomUUID().toString)

    val program = Resource.make(client.producerAsync(ProducerConfig(topic)))(_.closeAsync).use {
      _.sendAsync("test_message")
    }
    val checkAvailable = Resource.make(client.readerAsync(ReaderConfig(topic, startMessage = Message(MessageId.earliest))))(_.closeAsync).use {
      _.hasMessageAvailableAsync
    }
    eventually {
      checkAvailable.unsafeRunSync() shouldBe false
    }
    (program *> checkAvailable).unsafeRunSync() shouldBe true
  }

  def processResource[F[_] : Async : AsyncHandler, X](
    pulsarResource: Resource[F, (Producer[String], Consumer[String])])(
    pr: (Producer[String], Consumer[String]) => F[X]
  ): F[X] =
    pulsarResource.use {
      case (producer, consumer) =>
        pr(producer, consumer)
    }

  test("async batch methods should return batch instead of single message") {
    import CatsAsyncHandler._
    val msg = "hello_batch"
    val topic = Topic("persistent://sample/standalone/ns1/batch_cats_test")
    val subscription = Subscription("batch_cats_test")

    processResource[IO, Vector[ConsumerMessage[String]]](syncResources[IO](client, topic, subscription)) { (producer, consumer) =>
      asyncBatchProgram[IO](producer, consumer, msg)
    }.map(_.length).unsafeRunSync() should be > 1

    processResource[IO, Vector[ConsumerMessage[String]]](asyncResources[IO](client, topic, subscription)) { (producer, consumer) =>
      asyncBatchProgram[IO](producer, consumer, msg)
    }.map(_.length).unsafeRunSync() should be > 1
  }

  test("async client methods should work with any monad which implements Async - IO") {
    import CatsAsyncHandler._
    val msg = "hello cats-effect IO"
    val topic = Topic("persistent://sample/standalone/ns1/cats_async_io")
    val subscription = Subscription("cats_effect_test_IO")

    processResource[IO, ConsumerMessage[String]](syncResources[IO](client, topic, subscription)) { (producer, consumer) =>
      asyncProgram[IO](producer, consumer, msg)
    }.map(_.value).unsafeRunSync() shouldBe msg

    processResource[IO, ConsumerMessage[String]](asyncResources[IO](client, topic, subscription)) { (producer, consumer) =>
      asyncProgram[IO](producer, consumer, msg)
    }.map(_.value).unsafeRunSync() shouldBe msg
  }

  test("async client methods should work with any monad which implements Async - StateT[IO, Int, ?]") {
    import CatsAsyncHandler._
    type F[T] = StateT[IO, Int, T] // this would be `StateT[IO, Int, ?]` with kind projector
    val msg = "hello cats-effect monad transformers"
    val topic = Topic("persistent://sample/standalone/ns1/cats_async_statet_io")
    val subscription = Subscription("cats_effect_test_StateT_IO")
    val rnd = Random.nextInt()

    processResource[IO, (Int, ConsumerMessage[String])](syncResources[IO](client, topic, subscription)) { (producer, consumer) =>
      asyncProgram[F](producer, consumer, msg).run(rnd)
    }.map { case (a, msg) => (a, msg.value) }.unsafeRunSync() shouldBe(rnd, msg)

    processResource[IO, (Int, ConsumerMessage[String])](asyncResources[IO](client, topic, subscription)) { (producer, consumer) =>
      asyncProgram[F](producer, consumer, msg).run(rnd)
    }.map { case (a, msg) => (a, msg.value) }.unsafeRunSync() shouldBe(rnd, msg)
  }

  test("async client methods should work with any monad which implements Async - Monix Task") {
    import CatsAsyncHandler._
    import monix.eval.Task
    import monix.execution.Scheduler.Implicits.global
    val msg = "hello monix via cats-effect"
    val topic = Topic("persistent://sample/standalone/ns1/cats_async_monix_task")
    val subscription = Subscription("cats_effect_test_monix_task")

    processResource[Task, ConsumerMessage[String]](syncResources[Task](client, topic, subscription)) { (producer, consumer) =>
      asyncProgram[Task](producer, consumer, msg)
    }.map(_.value).runSyncUnsafe() shouldBe msg

    processResource[Task, ConsumerMessage[String]](asyncResources[Task](client, topic, subscription)) { (producer, consumer) =>
      asyncProgram[Task](producer, consumer, msg)
    }.map(_.value).runSyncUnsafe() shouldBe msg
  }

  test("async client methods should work with any monad which implements Async - ZIO") {
    import CatsAsyncHandler._
    import zio.interop.catz._
    import zio.{Runtime, Task}

    val msg = "hello ZIO via cats-effect"
    val topic = Topic("persistent://sample/standalone/ns1/cats_async_zio_task")
    val subscription = Subscription("cats_effect_test_zio_task")

    val sync = processResource[Task, ConsumerMessage[String]](syncResources[Task](client, topic, subscription)) { (producer, consumer) =>
      asyncProgram[Task](producer, consumer, msg)
    }.map(_.value)

    val async = processResource[Task, ConsumerMessage[String]](asyncResources[Task](client, topic, subscription)) { (producer, consumer) =>
      asyncProgram[Task](producer, consumer, msg)
    }.map(_.value)

    Runtime.default.unsafeRun(sync) shouldBe msg
    Runtime.default.unsafeRun(async) shouldBe msg
  }
}
