package com.sksamuel.pulsar4s.streams

import java.util.UUID

import com.sksamuel.pulsar4s._
import org.reactivestreams.{Publisher, Subscriber}

/**
  * A Pulsar publisher will attach to a pulsar instance and publish
  * the messages on a topic.
  *
  * @param consumer a consumer instance to recieve messages from
  */
class PulsarPublisher(client: PulsarClient, topic: Topic) extends Publisher[Message] {

  override def subscribe(s: Subscriber[_ >: Message]): Unit = {
    // Rule 1.9 subscriber cannot be null
    if (s == null) throw new NullPointerException("Rule 1.9: Subscriber cannot be null")

    val consumer = client.consumer(topic, Subscription("pulsub_" + UUID.randomUUID))
    val subscription = new PulsarSubscription(consumer)

    s.onSubscribe(subscription)
    // rule 1.03 the subscription should not invoke any onNext's until the onSubscribe call has returned
    // even tho the user might call request in the onSubscribe, we can't start sending the results yet.
    // this ready method signals to the actor that its ok to start sending data.
    subscription.ready()
  }

}

class PulsarSubscription(consumer: Consumer) extends org.reactivestreams.Subscription[Message] {

  private var requested = 0L

  override def cancel(): Unit = {

  }

  override def request(n: Long): Unit = {
    requested = requested + n
  }

  private[pulsar4s] def ready(): Unit = {

  }
}