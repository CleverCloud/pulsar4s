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

  override def createElement(element: Int): Item = castles(element)

  val castles = Array(
    Item("bodium"),
    Item("hever"),
    Item("tower of london"),
    Item("canarvon"),
    Item("conwy"),
    Item("beaumaris"),
    Item("bolsover"),
    Item("conningsbrough"),
    Item("tintagel"),
    Item("rochester"),
    Item("dover"),
    Item("hexham"),
    Item("harleigh"),
    Item("white"),
    Item("radley"),
    Item("berkeley")
  )
}

case class Item(name: String)
