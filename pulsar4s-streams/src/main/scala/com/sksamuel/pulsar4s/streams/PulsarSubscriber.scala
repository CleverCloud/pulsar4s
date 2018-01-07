package com.sksamuel.pulsar4s.streams

import com.sksamuel.pulsar4s.Message
import org.reactivestreams
import org.reactivestreams.Subscriber

class PulsarSubscriber extends Subscriber[Message] {

  override def onError(t: Throwable): Unit = {
    if (t == null) throw new NullPointerException()
    else throw t
  }

  override def onComplete(): Unit = ???
  override def onNext(t: Message): Unit = ???
  override def onSubscribe(s: reactivestreams.Subscription): Unit = ???
}
