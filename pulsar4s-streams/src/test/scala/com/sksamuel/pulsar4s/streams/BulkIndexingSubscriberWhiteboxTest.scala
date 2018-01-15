package com.sksamuel.pulsar4s.streams

import java.util.UUID

import com.sksamuel.pulsar4s.{Message, PulsarClient, Topic}
import org.reactivestreams.tck.SubscriberWhiteboxVerification.{SubscriberPuppet, WhiteboxSubscriberProbe}
import org.reactivestreams.tck.{SubscriberWhiteboxVerification, TestEnvironment}
import org.reactivestreams.{Subscriber, Subscription}
import org.scalatest.testng.TestNGSuiteLike

class BulkIndexingSubscriberWhiteboxTest
  extends SubscriberWhiteboxVerification[Message](new TestEnvironment(DEFAULT_TIMEOUT_MILLIS))
    with TestNGSuiteLike {

  private val client = PulsarClient("pulsar://localhost:6650", "sample/standalone/ns1")
  // we use a random topic so that previous runs don't affect us
  private val topic = Topic("persistent://sample/standalone/ns1/reactivepub_" + UUID.randomUUID())
  private val producer = client.producer(topic)

  override def createSubscriber(probe: WhiteboxSubscriberProbe[Message]): Subscriber[Message] = {
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

      override def onNext(t: Message): Unit = {
        super.onNext(t)
        probe.registerOnNext(t)
      }
    }
  }

  override def createElement(element: Int): Message = castles(element)
}