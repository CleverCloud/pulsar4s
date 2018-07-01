package com.sksamuel.pulsar4s.streams

import java.util.concurrent.atomic.AtomicBoolean
import java.util.concurrent.{Executors, LinkedBlockingQueue}

import com.sksamuel.pulsar4s.Producer
import org.reactivestreams.{Subscriber, Subscription}

import scala.collection.JavaConverters._

/**
  * A [[PulsarSubscriber]] will subscribe to a reactive streams
  * publisher, and for each message it receives, it will publish
  * that message to a pulsar topic.
  */
class PulsarSubscriber[T](producer: Producer[T],
                          bufferSize: Int = Int.MaxValue) extends Subscriber[T] {

  private val buffer = new LinkedBlockingQueue[T](bufferSize)
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

  override def onNext(t: T): Unit = {
    buffer.put(t)
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
              for (t <- buffer.iterator.asScala) {
                producer.send(t)
              }
              buffer.clear()
              running.set(false)
            // if we haven't yet received onComplete then we should just
            // continue to take or block
            case false =>
              val t = buffer.take()
              producer.send(t)
          }
        }
      }
    })
    executor.shutdown()
  }
}
