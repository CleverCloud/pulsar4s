package com.sksamuel.pulsar4s.monixs

import java.util.UUID

import com.sksamuel.pulsar4s._
import org.apache.pulsar.client.api.Schema
import org.scalatest.BeforeAndAfterAll

import scala.concurrent.Await
import scala.concurrent.duration.Duration
import org.scalatest.funsuite.AnyFunSuite
import org.scalatest.matchers.should.Matchers

class MonixAsyncHandlerTest extends AnyFunSuite with Matchers with BeforeAndAfterAll {

  import monix.execution.Scheduler.Implicits.global
  import MonixAsyncHandler._

  implicit val schema: Schema[String] = Schema.STRING

  val client = PulsarClient("pulsar://localhost:6650")
  val topic = Topic("persistent://sample/standalone/ns1/monix_" + UUID.randomUUID())

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
    val zipped = r.toString.split(":") zip value.messageId.toString.split(":")
    zipped.foreach(t => t._1 shouldBe t._2)
    consumer.close()
  }
}
