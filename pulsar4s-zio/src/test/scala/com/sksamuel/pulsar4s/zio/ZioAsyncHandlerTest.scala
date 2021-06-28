package com.sksamuel.pulsar4s.zio

import java.util.UUID
import com.sksamuel.pulsar4s.{ConsumerConfig, ProducerConfig, PulsarClient, Subscription, Topic}
import org.apache.pulsar.client.api.Schema
import org.scalatest.BeforeAndAfterAll
import org.scalatest.concurrent.Eventually
import org.scalatest.funsuite.AnyFunSuite
import org.scalatest.matchers.should.Matchers

class ZioAsyncHandlerTest extends AnyFunSuite with Matchers with BeforeAndAfterAll with Eventually {

  import ZioAsyncHandler._

  implicit val schema: Schema[String] = Schema.STRING

  private val client = PulsarClient("pulsar://localhost:6650")
  private val topic = Topic("persistent://sample/standalone/ns1/zio_" + UUID.randomUUID())

  override def afterAll(): Unit = {
    client.close()
  }

  test("async producer should use zio") {
    val producer = client.producer(ProducerConfig(topic))
    val t = producer.sendAsync("wibble")
    val r = zio.Runtime.default.unsafeRun(t.either)
    r.right.get should not be null
    producer.close()
  }

  test("async consumer should use zio") {
    val consumer = client.consumer(ConsumerConfig(topics = Seq(topic), subscriptionName = Subscription("mysub_" + UUID.randomUUID())))
    consumer.seekEarliest()
    val t = consumer.receiveAsync
    val r = zio.Runtime.default.unsafeRun(t.either)
    r shouldBe Symbol("right")
    new String(r.right.get.data) shouldBe "wibble"
    consumer.close()
  }

  test("async consumer getMessageById should use zio") {
    val consumer = client.consumer(ConsumerConfig(topics = Seq(topic), subscriptionName = Subscription("mysub_" + UUID.randomUUID())))
    consumer.seekEarliest()
    val receive = consumer.receiveAsync
    eventually {
      val value = zio.Runtime.default.unsafeRun(receive.either)
      val t = consumer.getLastMessageIdAsync
      val r = zio.Runtime.default.unsafeRun(t.either)
      val zipped = r.right.get.toString.split(":") zip value.right.get.messageId.toString.split(":")
      zipped.foreach(t => t._1 shouldBe t._2)
    }
    consumer.close()
  }
}
