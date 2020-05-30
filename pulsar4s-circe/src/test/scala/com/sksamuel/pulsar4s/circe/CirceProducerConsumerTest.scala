package com.sksamuel.pulsar4s.circe

import java.util.UUID

import com.sksamuel.pulsar4s._
import io.circe.generic.auto._
import io.circe.CursorOp.DownField
import io.circe.DecodingFailure
import org.scalatest.funsuite.AnyFunSuite
import org.scalatest.matchers.should.Matchers

import scala.util.Failure

class CirceProducerConsumerTest extends AnyFunSuite with Matchers {

  test("producer and consumer synchronous round trip") {

    val client = PulsarClient("pulsar://localhost:6650")
    val topic = Topic("persistent://sample/standalone/ns1/test_" + UUID.randomUUID)

    val producer = client.producer[Cafe](ProducerConfig(topic))
    val cafe = Cafe("le table", Place(1, "Paris"))
    val messageId = producer.send(cafe)
    producer.close()

    val consumer = client.consumer[Cafe](ConsumerConfig(topics = Seq(topic), subscriptionName = Subscription.generate))
    consumer.seek(MessageId.earliest)
    val msg = consumer.receive
    msg.get.value shouldBe cafe
    consumer.close()

    client.close()
  }

  test("producer and consumer synchronous round trip with failed deserialization") {

    val client = PulsarClient("pulsar://localhost:6650")
    val topic = Topic("persistent://sample/standalone/ns1/test_" + UUID.randomUUID)

    val producer = client.producer[String](ProducerConfig(topic))
    val messageId = producer.send("""{"foo": "bar"}""")
    producer.close()

    val consumer = client.consumer[Cafe](ConsumerConfig(topics = Seq(topic), subscriptionName = Subscription.generate))
    consumer.seek(MessageId.earliest)
    val msg = consumer.receive
    msg.get.valueTry shouldBe Failure(DecodingFailure("Attempt to decode value on failed cursor", List(DownField("name"))))
    consumer.close()

    client.close()
  }

}
