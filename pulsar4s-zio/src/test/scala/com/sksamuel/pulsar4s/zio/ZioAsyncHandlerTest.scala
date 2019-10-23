package com.sksamuel.pulsar4s.zio

import java.util.UUID

import com.sksamuel.pulsar4s.{ConsumerConfig, ProducerConfig, PulsarClient, Subscription, Topic}
import org.apache.pulsar.client.api.Schema
import org.scalatest.{BeforeAndAfterAll, FunSuite, Matchers}
import zio.DefaultRuntime

import scala.concurrent.Await
import scala.concurrent.duration.Duration

class ZioAsyncHandlerTest extends FunSuite with Matchers with BeforeAndAfterAll with DefaultRuntime {

  import ZioAsyncHandler._

  implicit val schema: Schema[String] = Schema.STRING

  val client = PulsarClient("pulsar://localhost:6650")
  val topic = Topic("persistent://sample/standalone/ns1/monix_" + UUID.randomUUID())

  override def afterAll(): Unit = {
    client.close()
  }

  test("async producer should use zio") {
    val producer = client.producer(ProducerConfig(topic))
    val t = producer.sendAsync("wibble")
    val f = unsafeRunToFuture(t)
    Await.result(f, Duration.Inf) should not be null
    producer.close()
  }

  test("async consumer should use zio") {
    val consumer = client.consumer(ConsumerConfig(topics = Seq(topic), subscriptionName = Subscription("mysub_" + UUID.randomUUID())))
    consumer.seekEarliest()
    val t = consumer.receiveAsync
    val f = unsafeRunToFuture(t)
    new String(Await.result(f, Duration.Inf).data) shouldBe "wibble"
    consumer.close()
  }
}
