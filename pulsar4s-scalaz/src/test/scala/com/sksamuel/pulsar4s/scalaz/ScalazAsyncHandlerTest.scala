package com.sksamuel.pulsar4s.scalaz

import java.util.UUID

import com.sksamuel.pulsar4s._
import org.apache.pulsar.client.api.Schema
import org.scalatest.BeforeAndAfterAll
import org.scalatest.funsuite.AnyFunSuite
import org.scalatest.matchers.should.Matchers

class ScalazAsyncHandlerTest extends AnyFunSuite with Matchers with BeforeAndAfterAll {

  implicit val schema: Schema[String] = Schema.STRING

  val client = PulsarClient("pulsar://localhost:6650")
  val topic = Topic("persistent://sample/standalone/ns1/scalaz_" + UUID.randomUUID())

  override def afterAll(): Unit = {
    client.close()
  }

  import ScalazAsyncHandler._

  test("async producer should use scalaz task") {
    val producer = client.producer(ProducerConfig(topic))
    val t = producer.sendAsync("wibble")
    t.unsafePerformSync should not be null
    producer.close()
  }

  test("async consumer should use scalaz task") {
    val consumer = client.consumer(ConsumerConfig(topics = Seq(topic), subscriptionName = Subscription.generate)
    )
    consumer.seekEarliest()
    val t = consumer.receiveAsync
    new String(t.unsafePerformSync.data) shouldBe "wibble"
    consumer.close()
  }

  test("async consumer getMessageById should use scalaz task") {
    val consumer = client.consumer(ConsumerConfig(topics = Seq(topic), subscriptionName = Subscription.generate)
    )
    consumer.seekEarliest()
    val receive = consumer.receiveAsync
    val value = receive.unsafePerformSync
    val t = consumer.getLastMessageIdAsync
    val r = t.unsafePerformSync
    val zipped = r.toString.split(":") zip value.messageId.toString.split(":")
    zipped.foreach(t => t._1 shouldBe t._2)
    consumer.close()
  }
}
