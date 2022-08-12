package com.sksamuel.pulsar4s.akka

import akka.Done
import akka.stream.scaladsl.{Sink, Source}
import com.sksamuel.pulsar4s.{Consumer, Reader, ConsumerMessage, MessageId, Producer, ProducerMessage, Topic}

import scala.concurrent.Future
import scala.concurrent.duration._

package object streams {

  private[streams] val DefaultCloseDelay = 30.seconds

  /**
   * Create an Akka Streams source for the given [[Consumer]] that produces [[ConsumerMessage]]s and auto-acknowledges.
   *
   * @param create a function to create a new [[Consumer]].
   * @param seek an optional [[MessageId]] to seek to. Note that seeking will not work on multi-topic subscriptions.
   *             Prefer setting `subscriptionInitialPosition` in `ConsumerConfig` instead if you need to start at the
   *             earliest or latest offset.
   * @return the new [[Source]].
   */
  def source[T](create: () => Consumer[T], seek: Option[MessageId] = None): Source[ConsumerMessage[T], Control] =
    Source.fromGraph(new PulsarSourceGraphStage(create, seek))

  /**
    * Create an Akka Streams source for the given [[Reader]] that produces [[ConsumerMessage]]s.
    * Readers are used when there is no needs to track message consumption
    *
    * @param create a function to create a new [[Reader]].
    * @param seek an optional [[MessageId]] to seek to.
    * @return the new [[Source]].
    */
  def sourceReader[T](create: () => Reader[T], seek: Option[MessageId] = None): Source[ConsumerMessage[T], Control] =
    Source.fromGraph(new PulsarReaderSourceGraphStage(create, seek))

  /**
   * Create an Akka Streams source for the given [[Consumer]] that produces [[CommittableMessage]]s, which can be
   * acknowledged individually.
   *
   * @param create a function to create a new [[Consumer]].
   * @param seek an optional [[MessageId]] to seek to. Note that seeking will not work on multi-topic subscriptions.
   *             Prefer setting `subscriptionInitialPosition` in `ConsumerConfig` instead if you need to start at the
   *             earliest or latest offset.
   * @param closeDelay the maximum amount of time to wait after the source completes before closing the consumer,
   *                   assuming the consumer is not already closed explicitly by `shutdown` or `drainAndShutdown`.
   * @return the new [[Source]].
   */
  def committableSource[T](
    create: () => Consumer[T],
    seek: Option[MessageId] = None,
    closeDelay: FiniteDuration = DefaultCloseDelay,
  ): Source[CommittableMessage[T], Control] = {
    Source.fromGraph(new PulsarCommittableSourceGraphStage[T](create, seek, closeDelay))
  }

  @deprecated("added for binary compatibility", "2.7.1")
  private[streams] def committableSource[T](
    create: () => Consumer[T],
    seek: Option[MessageId],
  ): Source[CommittableMessage[T], Control] = committableSource(create, seek, 30.seconds)

  /**
   * Create an Akka Streams sink from a [[Producer]].
   *
   * @param create a function to create a new [[Producer]]
   * @return the new [[Sink]].
   */
  def sink[T](create: () => Producer[T]): Sink[ProducerMessage[T], Future[Done]] =
    Sink.fromGraph(new PulsarSinkGraphStage(create))


  /**
   * Create a multi-topic Akka Streams sink from a [[Producer]].
   *
   * @param create a function to create a new [[Producer]] taking [[com.sksamuel.pulsar4s.Topic]] as a parameter
   * @return the new [[Sink]].
   */
  def multiSink[T](create: Topic => Producer[T], initTopics: Iterable[Topic] = Seq.empty):
  Sink[(Topic, ProducerMessage[T]), Future[Done]] =
    Sink.fromGraph(new PulsarMultiSinkGraphStage(create, initTopics.toSet))
}
