package com.sksamuel.pulsar4s.zio

import java.util.UUID
import com.sksamuel.pulsar4s.{ConsumerConfig, MessageId, ProducerConfig, PulsarClient, PulsarClientConfig, Subscription, Topic}
import org.apache.pulsar.client.api.Schema
import org.scalatest.BeforeAndAfterAll
import org.scalatest.concurrent.Eventually
import org.scalatest.funsuite.AnyFunSuite
import org.scalatest.matchers.should.Matchers
import zio.{Exit, Task, Trace, Unsafe}

import scala.concurrent.duration._

class ZioAsyncHandlerTest extends AnyFunSuite with Matchers with BeforeAndAfterAll with Eventually {

  import ZioAsyncHandler._

  implicit val schema: Schema[String] = Schema.STRING

  private val client = PulsarClient(PulsarClientConfig(
    serviceUrl = "pulsar://localhost:6650",
    enableTransaction = Some(true)
  ))
  private val topic = Topic("persistent://sample/standalone/ns1/zio_" + UUID.randomUUID())

  override def afterAll(): Unit = {
    client.close()
  }

  test("async producer should use zio") {
    val producer = client.producer(ProducerConfig(topic))
    val t = producer.sendAsync("wibble")
    val r = Unsafe.unsafe(implicit unsafe => zio.Runtime.default.unsafe.run(t.either.map(_.toOption.get)).getOrThrowFiberFailure())
    r should not be null
    producer.close()
  }

  test("async consumer should use zio") {
    val consumer = client.consumer(ConsumerConfig(topics = Seq(topic), subscriptionName = Subscription("mysub_" + UUID.randomUUID())))
    consumer.seekEarliest()
    val t = consumer.receiveAsync
    val r = Unsafe.unsafe(implicit unsafe => zio.Runtime.default.unsafe.run(t.either.map(_.toOption.get)).getOrThrowFiberFailure())
    new String(r.data) shouldBe "wibble"
    consumer.close()
  }

  test("async consumer getMessageById should use zio") {
    val consumer = client.consumer(ConsumerConfig(topics = Seq(topic), subscriptionName = Subscription("mysub_" + UUID.randomUUID())))
    consumer.seekEarliest()
    val receive = consumer.receiveAsync
    val value = Unsafe.unsafe(implicit unsafe => zio.Runtime.default.unsafe.run(receive.either.map(_.toOption.get)).getOrThrowFiberFailure())
    val t = consumer.getLastMessageIdAsync
    val r = Unsafe.unsafe(implicit unsafe => zio.Runtime.default.unsafe.run(t.either.map(_.toOption.get)).getOrThrowFiberFailure())
    r.entryId shouldBe value.messageId.entryId
    r.partitionIndex shouldBe value.messageId.partitionIndex
    consumer.close()
  }

  test("producer and consumer can execute a transaction using zio") {
    val producer = client.producer(ProducerConfig(topic, sendTimeout = Some(Duration.Zero)))
    val consumer = client.consumer(ConsumerConfig(topics = Seq(topic), subscriptionName = Subscription("mysub_" + UUID.randomUUID)))
    consumer.seekEarliest()
    val msgIdIO = client.transaction.withTimeout(1.second).runWith[MessageId, Task] { implicit txn =>
      for {
        msg <- consumer.receiveAsync
        msgId <- producer.tx.sendAsync(msg.value + "_test")
        _ <- consumer.tx.acknowledgeAsync(msg.messageId)
      } yield msgId
    }
    Unsafe.unsafe(implicit unsafe => zio.Runtime.default.unsafe.run(msgIdIO).getOrThrowFiberFailure())
    consumer.close()
    producer.close()
  }
}
