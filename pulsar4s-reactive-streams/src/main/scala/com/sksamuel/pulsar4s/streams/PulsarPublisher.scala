package com.sksamuel.pulsar4s.streams

import java.util.UUID
import java.util.concurrent.atomic.{AtomicBoolean, AtomicLong, AtomicReference}
import java.util.concurrent.{Executors, Semaphore}

import com.sksamuel.exts.Logging
import com.sksamuel.pulsar4s._
import org.reactivestreams.{Publisher, Subscriber}

import scala.util.control.NonFatal

/**
  * A Pulsar publisher will attach to a pulsar instance and publish
  * the messages on a topic.
  *
  * @param client    the pulsar client to create a consumer on
  * @param topic     the topic to read messages from
  * @param messageId the messageId to seek, to start from the beginning of the topic, use MessageId.earliest
  * @param max       the maximum number of messages to receive before terminating.
  *                  Set to Long.MaxValue for an unbounded number of messages.
  */
class PulsarPublisher(client: PulsarClient,
                      topic: Topic,
                      messageId: MessageId,
                      max: Long) extends Publisher[Message] {

  def this(client: PulsarClient, topic: Topic) = this(client, topic, MessageId.earliest, Long.MaxValue)

  override def subscribe(subscriber: Subscriber[_ >: Message]): Unit = {
    // Rule 1.9 subscriber cannot be null
    if (subscriber == null) throw new NullPointerException("Rule 1.9: Subscriber cannot be null")

    val consumer = client.consumer(topic, Subscription("pulsar_subscription_" + UUID.randomUUID))
    consumer.seek(messageId)

    val subscription = new PulsarSubscription(consumer, subscriber, max)

    // rule 1.03 the subscription should not invoke any onNext's until the onSubscribe call has returned.
    // Therefore we have to handle the situation where a user calls request(n) as part of the onSubscribe
    // callback. We can do this by delaying the sending of message until the onSubscribe callback returns
    subscriber.onSubscribe(subscription)
    subscription.start()
  }
}

class PulsarSubscription(consumer: Consumer,
                         subscriber: Subscriber[_ >: Message],
                         max: Long) extends org.reactivestreams.Subscription with Logging {

  private val executor = Executors.newSingleThreadExecutor()
  private val requested = new AtomicLong(0)
  private val sent = new AtomicLong(0)
  private val mutex = new Semaphore(0)
  private val error = new AtomicReference[Throwable](null)
  private val cancelled = new AtomicBoolean(false)

  override def cancel(): Unit = {
    // rule 3.7 after first cancel, rest must be no-ops
    if (cancelled.compareAndSet(false, true)) {
      executor.shutdownNow()
    }
  }

  // rule 3.3 says we shouldn't call onNext in here, or at least bound the possible recursion
  // rule 3.9: n must be positive
  override def request(n: Long): Unit = {
    if (n <= 0) {
      error.set(new IllegalArgumentException(s"3.9"))
      executor.shutdownNow()
    } else {
      // rule 3.06, after cancellation, requests must be no-ops
      if (!cancelled.get) {
        requested.addAndGet(n)
        mutex.release()
      }
    }
  }

  private[pulsar4s] def start(): Unit = {
    executor.submit(new Runnable {
      override def run(): Unit = {
        try {
          while (!Thread.currentThread.isInterrupted && sent.get() < max) {

            // at this point, we've sent everything requested, so we should sleep until more
            // requests come in. It's possible that a request came in after we exited the loop
            // but before we go to sleep, so we need to handle that by only sleeping if
            // there are no permits pending
            if (mutex.drainPermits() == 0)
              mutex.acquire()

            // send up to the amount requested; the requested total may increase while
            // we are looping if more requests come in; that's fine we'll process them all
            while (sent.get < requested.get && sent.get < max) {
              val msg = consumer.receive
              logger.trace(s"Message received $msg")
              subscriber.onNext(msg)
              sent.incrementAndGet()
            }
          }
        } catch {
          case _: InterruptedException => logger.debug("Subscription cancellation, consumer thread will now exit")
          case NonFatal(e) => error.set(e)
        } finally {
          error.get match {
            // rule 1.08 and https://github.com/reactive-streams/reactive-streams-jvm/issues/113
            // not clear from spec, but after cancel don't send onComplete
            case null if !cancelled.get =>
              logger.debug("Signalling oncomplete")
              // rule 1.05
              subscriber.onComplete()
            case null =>
              // nothing to do
            case t =>
              logger.error("Signalling onError", t)
              subscriber.onError(t)
          }
        }
      }
    })
  }
}