package com.sksamuel.pulsar4s

import java.util.UUID

import org.apache.pulsar.client.api.Schema
import org.scalatest.BeforeAndAfterAll

import scala.concurrent.Await
import scala.concurrent.duration._
import org.scalatest.funsuite.AnyFunSuite
import org.scalatest.matchers.should.Matchers
import scala.util.Random


class FutureAsyncHandlerTest extends AnyFunSuite with Matchers with BeforeAndAfterAll {

  import scala.concurrent.ExecutionContext.Implicits.global

  implicit val schema: Schema[String] = Schema.STRING

  private val client = PulsarClient(PulsarClientConfig(
    serviceUrl = "pulsar://localhost:6650",
    enableTransaction = Some(true)
  ))
  private val topic = Topic("persistent://sample/standalone/ns1/futureasync_" + UUID.randomUUID())

  override def afterAll(): Unit = {
    client.close()
  }

  test("async producer should bring future effect into scope by default") {
    val producer = client.producer(ProducerConfig(topic))
    val f = producer.sendAsync("wibble")
    Await.result(f, Duration.Inf) should not be null
    producer.close()
  }

  test("async consumer should bring future effect into scope by default") {
    val consumer = client.consumer(ConsumerConfig(topics = Seq(topic), subscriptionName = Subscription("mysub_" + UUID.randomUUID)))
    consumer.seekEarliest()
    val f = consumer.receiveAsync
    new String(Await.result(f, Duration.Inf).data) shouldBe "wibble"
    consumer.close()
  }

  test("async consumer getMessageById should bring future effect into scope by default") {
    val consumer = client.consumer(ConsumerConfig(topics = Seq(topic), subscriptionName = Subscription("mysub_" + UUID.randomUUID)))
    consumer.seekEarliest()
    val receive = consumer.receiveAsync
    val value = Await.result(receive, Duration.Inf)
    val t = consumer.getLastMessageIdAsync
    val r = Await.result(t, Duration.Inf)
    val zipped = r.toString.split(":") zip value.messageId.toString.split(":")
    zipped.foreach(t => t._1 shouldBe t._2)
    consumer.close()
  }

  test("async producer and consumer can participate in transaction") {
    val producer = client.producer(ProducerConfig(topic, sendTimeout = Some(Duration.Zero)))
    val consumer = client.consumer(ConsumerConfig(topics = Seq(topic), subscriptionName = Subscription("mysub_" + UUID.randomUUID)))
    consumer.seekEarliest()
    val msgIdFt = client.transaction.runWith { txn =>
      for {
        msg <- consumer.receiveAsync
        msgId <- txn(producer).sendAsync(msg.value + "_test")
        _ <- txn(consumer).acknowledgeAsync(msg.messageId)
      } yield {
        msgId
      }
    }
    Await.result(msgIdFt, Duration.Inf)
    consumer.close()
    producer.close()
  }

  test("async producer and consumer can participate in manually managed transaction") {
    val producer = client.producer(ProducerConfig(topic, sendTimeout = Some(Duration.Zero)))
    val consumer = client.consumer(ConsumerConfig(topics = Seq(topic), subscriptionName = Subscription("mysub_" + UUID.randomUUID)))
    consumer.seekEarliest()
    val msgIdFt = for {
      txn <- client.transaction.start
      msg <- consumer.receiveAsync
      msgId <- txn(producer).sendAsync(msg.value + "_test")
      _ <- txn(consumer).acknowledgeAsync(msg.messageId)
      _ <- txn.commit
    } yield {
      msgId
    }
    Await.result(msgIdFt, Duration.Inf)
    consumer.close()
    producer.close()
  }

  test("async producer and consumer can participate in transaction returning either") {
    val producer = client.producer(ProducerConfig(topic, sendTimeout = Some(Duration.Zero)))
    val consumer = client.consumer(ConsumerConfig(topics = Seq(topic), subscriptionName = Subscription("mysub_" + UUID.randomUUID)))
    consumer.seekEarliest()
    val msgIdEitherFt = client.transaction.withTimeout(timeout = 1.second).runWithEither { implicit txn =>
      for {
        msg <- consumer.receiveAsync
        msgId <- producer.tx.sendAsync(msg.value + "_test")
        _ <- consumer.tx.acknowledgeAsync(msg.messageId)
      } yield {
        if (Random.nextBoolean()) Right(msgId) else Left(s"failed: $msgId")
      }
    }
    Await.result(msgIdEitherFt, Duration.Inf)
    consumer.close()
    producer.close()
  }
}
