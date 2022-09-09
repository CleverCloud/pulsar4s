package com.sksamuel.pulsar4s.fs2

import cats.effect.{IO, Sync}
import cats.implicits._
import cats.effect.unsafe.implicits.global
import com.sksamuel.pulsar4s.{AsyncHandler, ConsumerConfig, Message, MessageId, ProducerConfig, ProducerMessage, PulsarAsyncClient, PulsarClient, ReaderConfig, Subscription, Topic}
import org.apache.pulsar.client.api.{Schema, SubscriptionInitialPosition}
import org.scalatest.BeforeAndAfterAll
import org.scalatest.funsuite.AnyFunSuite
import org.scalatest.matchers.should.Matchers

import java.util.UUID

class StreamsTest extends AnyFunSuite with Matchers with BeforeAndAfterAll {

  implicit val schema: Schema[String] = Schema.STRING

  private val client = PulsarClient("pulsar://localhost:6650")

  override def afterAll(): Unit = {
    client.close()
  }

  private def publishMessages[F[_]: Sync: AsyncHandler](client: PulsarAsyncClient, t: Topic, messages: List[String]): F[Unit] =
    for {
      producer <- client.producerAsync[String, F](ProducerConfig(
        topic = t
      ))
      _ <- messages
            .map(producer.sendAsync(_))
            .sequence
    } yield ()

  test("able to read from topic") {
    import com.sksamuel.pulsar4s.cats.CatsAsyncHandler._
    val topic = Topic("persistent://sample/standalone/ns1/" + UUID.randomUUID().toString)
    val messages = (0 to 10).map(p => s"TestMessage_$p").toList

    (for {
      _ <- publishMessages[IO](client, topic, messages)
      read <- PulsarStreams.reader[IO, String](client.readerAsync[String, IO](ReaderConfig(
        topic = topic,
        startMessage = Message(MessageId.earliest)
      ))).take(messages.size).map(_.value).compile.toList
    } yield read shouldBe messages).unsafeRunSync()
  }

  test("able to read from subscription [batch]") {
    import com.sksamuel.pulsar4s.cats.CatsAsyncHandler._
    val topic = Topic("persistent://sample/standalone/ns1/" + UUID.randomUUID().toString)
    val messages = (0 to 10).map(p => s"TestMessage_$p").toList

    (for {
      _ <- publishMessages[IO](client, topic, messages)
      batch <- PulsarStreams.batch[IO, String](client.consumerAsync[String, IO](ConsumerConfig(
        subscriptionName = Subscription("fs2_subscription_batch"),
        topics = Seq(topic),
        subscriptionInitialPosition = Some(SubscriptionInitialPosition.Earliest)
      ))).take(messages.size).map(_.data.value).compile.toList
      single <- PulsarStreams.single[IO, String](client.consumerAsync[String, IO](ConsumerConfig(
        subscriptionName = Subscription("fs2_subscription_single"),
        topics = Seq(topic),
        subscriptionInitialPosition = Some(SubscriptionInitialPosition.Earliest)
      ))).take(messages.size).map(_.data.value).compile.toList
    } yield {
      batch shouldBe messages
      single shouldBe messages
    }).unsafeRunSync()
  }

  test("able to connect with sink") {
    import com.sksamuel.pulsar4s.cats.CatsAsyncHandler._

    val topic = Topic("persistent://sample/standalone/ns1/" + UUID.randomUUID().toString)
    val topic2 = Topic("persistent://sample/standalone/ns1/" + UUID.randomUUID().toString)
    val messages = (0 to 10).map(p => s"TestMessage_$p").toList

    (for {
      _ <- publishMessages[IO](client, topic, messages)

      _ <- PulsarStreams.batch[IO, String](client.consumerAsync[String, IO](ConsumerConfig(
        subscriptionName = Subscription("fs2_subscription_batch"),
        topics = Seq(topic),
        subscriptionInitialPosition = Some(SubscriptionInitialPosition.Earliest)
      )))
        .take(messages.size)
        .map(_.map { message =>
          ProducerMessage(message.value)
        })
        .through(PulsarStreams.committableSink(client.producerAsync[String, IO](ProducerConfig(topic2))))
        .compile
        .drain

      batch <- PulsarStreams.batch[IO, String](client.consumerAsync[String, IO](ConsumerConfig(
        subscriptionName = Subscription("fs2_subscription_batch"),
        topics = Seq(topic2),
        subscriptionInitialPosition = Some(SubscriptionInitialPosition.Earliest)
      ))).take(messages.size).map(_.data.value).compile.toList
    } yield {
      batch shouldBe messages
    }).unsafeRunSync()
  }
}
