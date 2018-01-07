package com.sksamuel.pulsar4s.streams

import com.sksamuel.pulsar4s.Message
import org.reactivestreams.tck.SubscriberWhiteboxVerification.{SubscriberPuppet, WhiteboxSubscriberProbe}
import org.reactivestreams.tck.{SubscriberWhiteboxVerification, TestEnvironment}
import org.reactivestreams.{Subscriber, Subscription}

class BulkIndexingSubscriberWhiteboxTest
  extends SubscriberWhiteboxVerification[Message](new TestEnvironment(DEFAULT_TIMEOUT_MILLIS)) {

  //implicit val system = ActorSystem()

  override def createSubscriber(probe: WhiteboxSubscriberProbe[Message]): Subscriber[Message] = {
    new PulsarSubscriber {

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

  val castles = Array(
    Message("bodium"),
    Message("hever"),
    Message("tower of london"),
    Message("canarvon"),
    Message("conwy"),
    Message("beaumaris"),
    Message("bolsover"),
    Message("conningsbrough"),
    Message("tintagel"),
    Message("rochester"),
    Message("dover"),
    Message("hexham"),
    Message("harleigh"),
    Message("white"),
    Message("radley"),
    Message("berkeley")
  )
}

case class Item(name: String)
