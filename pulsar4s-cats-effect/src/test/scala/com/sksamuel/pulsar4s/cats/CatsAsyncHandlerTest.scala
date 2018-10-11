package com.sksamuel.pulsar4s.cats

import java.util.UUID

import com.sksamuel.pulsar4s._
import org.apache.pulsar.client.api.Schema
import org.scalatest.{BeforeAndAfterAll, FunSuite, Matchers}

class CatsAsyncHandlerTest extends FunSuite with Matchers with BeforeAndAfterAll {

  import CatsAsyncHandler._

  implicit val schema: Schema[String] = Schema.STRING

  val client = PulsarClient("pulsar://localhost:6650")
  val topic = Topic("persistent://sample/standalone/ns1/cats_" + UUID.randomUUID())

  override def afterAll(): Unit = {
    client.close()
  }

  test("async producer should use cats") {
    val producer = client.producer(ProducerConfig(topic))
    val t = producer.sendAsync("wibble")
    t.unsafeRunSync() should not be null
    producer.close()
  }

  test("async consumer should use cats") {
    val consumer = client.consumer(ConsumerConfig(topics = Seq(topic), subscriptionName = Subscription("mysub_" + UUID.randomUUID)))
    consumer.seekEarliest()
    val t = consumer.receiveAsync
    new String(t.unsafeRunSync().data) shouldBe "wibble"
    consumer.close()
  }
}
