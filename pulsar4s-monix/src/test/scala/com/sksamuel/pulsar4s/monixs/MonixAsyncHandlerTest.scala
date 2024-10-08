package com.sksamuel.pulsar4s.monixs

import java.util.UUID

import com.sksamuel.pulsar4s._
import org.apache.pulsar.client.api.Schema
import org.scalatest.BeforeAndAfterAll

import scala.concurrent.Await
import scala.concurrent.duration._
import org.scalatest.funsuite.AnyFunSuite
import org.scalatest.matchers.should.Matchers

class MonixAsyncHandlerTest extends AnyFunSuite with Matchers with BeforeAndAfterAll {

  import monix.execution.Scheduler.Implicits.global
  import MonixAsyncHandler._

  implicit val schema: Schema[String] = Schema.STRING

  private val client: PulsarAsyncClient = PulsarClient(PulsarClientConfig(
    serviceUrl = "pulsar://localhost:6650",
    enableTransaction = Some(true)
  ))
  val topic: Topic = Topic("persistent://sample/standalone/ns1/monix_" + UUID.randomUUID())

  override def afterAll(): Unit = {
    client.close()
  }

  test("async producer should use monix") {
    val producer = client.producer(ProducerConfig(topic))
    val t = producer.sendAsync("wibble")
    val f = t.runToFuture
    Await.result(f, Duration.Inf) should not be null
    producer.close()
  }

  test("async consumer should use monix") {
    val consumer = client.consumer(ConsumerConfig(topics = Seq(topic), subscriptionName = Subscription("mysub_" + UUID.randomUUID())))
    consumer.seekEarliest()
    val t = consumer.receiveAsync
    val f = t.runToFuture
    new String(Await.result(f, Duration.Inf).data) shouldBe "wibble"
    consumer.close()
  }

  test("async consumer getMessageById should use monix") {
    val consumer = client.consumer(ConsumerConfig(topics = Seq(topic), subscriptionName = Subscription("mysub_" + UUID.randomUUID())))
    consumer.seekEarliest()
    val receive = consumer.receiveAsync
    val valueFuture = receive.runToFuture
    val value = Await.result(valueFuture, Duration.Inf)
    val t = consumer.getLastMessageIdAsync
    val rFuture = t.runToFuture
    val r = Await.result(rFuture, Duration.Inf)
    r.entryId shouldBe value.messageId.entryId
    r.partitionIndex shouldBe value.messageId.partitionIndex
    r.batchIndex shouldBe value.messageId.batchIndex
    consumer.close()
  }

  test("producer and consumer can execute a transaction using cats") {
    val producer = client.producer(ProducerConfig(topic, sendTimeout = Some(Duration.Zero)))
    val consumer = client.consumer(ConsumerConfig(topics = Seq(topic), subscriptionName = Subscription("mysub_" + UUID.randomUUID)))
    consumer.seekEarliest()
    val msgIdIO = client.transaction.withTimeout(1.second).runWith { implicit txn =>
      for {
        msg <- consumer.receiveAsync
        msgId <- producer.tx.sendAsync(msg.value + "_test")
        _ <- consumer.tx.acknowledgeAsync(msg.messageId)
      } yield msgId
    }
    Await.result(msgIdIO.runToFuture, Duration.Inf)
    consumer.close()
  }
}
