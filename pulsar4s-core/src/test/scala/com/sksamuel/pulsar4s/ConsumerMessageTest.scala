package com.sksamuel.pulsar4s

import com.sksamuel.pulsar4s.conversions.collections._
import org.apache.pulsar.client.api.Schema
import org.apache.pulsar.client.impl.MessageImpl
import org.apache.pulsar.common.api.proto.MessageMetadata
import org.apache.pulsar.shade.io.netty.buffer.Unpooled
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
      replicatedFrom = Some("reppy"),
      () => new MessageImpl[String](
        "test",
        MessageId.latest.toString,
        Map.empty[String, String].asJava,
        Unpooled.wrappedBuffer("foo".getBytes),
        Schema.STRING,
        new MessageMetadata()
      )
    )
    val java = ConsumerMessage.toJava(message, Schema.STRING)
    java.getPublishTime shouldBe 222
    java.getEventTime shouldBe 444
    java.getReplicatedFrom shouldBe "reppy"
    java.getTopicName shouldBe "t"
    java.getProducerName shouldBe "producer mcprodface"
  }
}
