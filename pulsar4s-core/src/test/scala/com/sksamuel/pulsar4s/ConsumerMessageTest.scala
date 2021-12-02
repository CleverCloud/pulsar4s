package com.sksamuel.pulsar4s

import org.apache.pulsar.client.api.Schema
import org.scalatest.BeforeAndAfterAll
import org.scalatest.funsuite.AnyFunSuite
import org.scalatest.matchers.should.Matchers

import scala.util.Try

class ConsumerMessageTest extends AnyFunSuite with Matchers with BeforeAndAfterAll {

  test("ConsumerMessage.toJava should set metadata") {
    val message = ConsumerMessageWithValueTry(
      key = None,
      valueTry = Try("foo"),
      data = "foo".getBytes,
      props = Map.empty,
      messageId = MessageId.latest,
      sequenceId = SequenceId(1),
      producerName = ProducerName("producer mcprodface"),
      publishTime = PublishTime(222),
      eventTime = EventTime(444),
      topic = Topic("t"),
      schemaVersion = Array.emptyByteArray,
      redeliveryCount = 2,
      replicatedFrom = Some("reppy")
    )
    val java = ConsumerMessage.toJava(message, Schema.STRING)
    java.getPublishTime shouldBe 222
    java.getEventTime shouldBe 444
    java.getReplicatedFrom shouldBe "reppy"
    java.getTopicName shouldBe "t"
    java.getProducerName shouldBe "producer mcprodface"
  }
}
