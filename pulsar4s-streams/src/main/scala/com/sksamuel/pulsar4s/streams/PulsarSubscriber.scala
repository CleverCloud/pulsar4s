package com.sksamuel.pulsar4s.streams

import java.util.concurrent.atomic.AtomicBoolean
import java.util.concurrent.{Executors, LinkedBlockingQueue}

import com.sksamuel.pulsar4s.{Message, Producer}
import org.reactivestreams.{Subscriber, Subscription}
import scala.collection.JavaConverters._

class PulsarSubscriber(producer: Producer,
                       bufferSize: Int = Int.MaxValue) extends Subscriber[Message] {

  private val buffer = new LinkedBlockingQueue[Message](bufferSize)
  private val executor = Executors.newSingleThreadExecutor()
  private val running = new AtomicBoolean(false)
  private val started = new AtomicBoolean(false)

  // flag determines when onComplete has been sent, which we can use
  // to determine if we should block on the buffer or exit
  private val completed = new AtomicBoolean(false)

  override def onSubscribe(sub: Subscription): Unit = {
    // rule 1.9 https://github.com/reactive-streams/reactive-streams-jvm#2.5
    // when the provided Subscriber is null it MUST throw a java.lang.NullPointerException to the caller
    if (sub == null) throw new NullPointerException()
    started.compareAndSet(false, true) match {
      case true => start()
      case false =>
        // rule 2.5, must cancel subscription if onSubscribe has been invoked twice
        // https://github.com/reactive-streams/reactive-streams-jvm#2.5
        sub.cancel()
        executor.shutdownNow()
    }
  }

  override def onNext(msg: Message): Unit = {
    buffer.put(msg)
  }

  override def onError(t: Throwable): Unit = {
    if (t == null) throw new NullPointerException()
    executor.shutdownNow()
  }

  override def onComplete(): Unit = {
    completed.set(true)
  }

  private def start(): Unit = {
    running.set(true)
    executor.submit(new Runnable {
      override def run(): Unit = {
        while (!Thread.currentThread().isInterrupted && running.get) {
          completed.get match {
            // if we have recieved onComplete then we just process the remaining
            // messages and then exit gracefully
            case true =>
              for (msg <- buffer.iterator.asScala) {
                producer.send(msg)
              }
              buffer.clear()
              running.set(false)
            // if we haven't yet received onComplete then we should just
            // continue to take or block
            case false =>
              val msg = buffer.take()
              producer.send(msg)
          }
        }
      }
    })
    executor.shutdown()
  }
}
