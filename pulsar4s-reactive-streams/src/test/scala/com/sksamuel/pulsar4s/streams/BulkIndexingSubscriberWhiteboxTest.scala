package com.sksamuel.pulsar4s.streams

import java.util.UUID

import com.sksamuel.pulsar4s.{ ProducerConfig, PulsarClient, Topic}
import org.apache.pulsar.client.api.Schema
import org.reactivestreams.tck.SubscriberWhiteboxVerification.{SubscriberPuppet, WhiteboxSubscriberProbe}
import org.reactivestreams.tck.{SubscriberWhiteboxVerification, TestEnvironment}
import org.reactivestreams.{Subscriber, Subscription}
import org.scalatest.BeforeAndAfterAll
import org.scalatest.testng.TestNGSuiteLike

class BulkIndexingSubscriberWhiteboxTest
  extends SubscriberWhiteboxVerification[String](new TestEnvironment(DEFAULT_TIMEOUT_MILLIS))
    with TestNGSuiteLike with BeforeAndAfterAll {

  override def afterAll(): Unit = {
    client.close()
  }

  private implicit val schema: Schema[String] = Schema.STRING

  private val client = PulsarClient("pulsar://localhost:6650")
  // we use a random topic so that previous runs don't affect us
  private val topic = Topic("persistent://sample/standalone/ns1/reactivepub_" + UUID.randomUUID())
  private val producer = client.producer[String](ProducerConfig(topic))

  override def createSubscriber(probe: WhiteboxSubscriberProbe[String]): Subscriber[String] = {
    new PulsarSubscriber(producer) {

      override def onSubscribe(s: Subscription): Unit = {
        super.onSubscribe(s)
        // register a successful Subscription, and create a Puppet,
        // for the WhiteboxVerification to be able to drive its tests:
        probe.registerOnSubscribe(new SubscriberPuppet() {

          def triggerRequest(elements: Long): Unit = {
            s.request(elements)
          }

          def signalCancel(): Unit = {
            s.cancel()
          }
        })
      }

      override def onComplete(): Unit = {
        super.onComplete()
        probe.registerOnComplete()
      }

      override def onError(t: Throwable): Unit = {
        probe.registerOnError(t)
      }

      override def onNext(t: String): Unit = {
        super.onNext(t)
        probe.registerOnNext(t)
      }
    }
  }

  override def createElement(element: Int): String = castles(element)
}