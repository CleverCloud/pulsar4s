package com.sksamuel.pulsar4s.akka

import akka.Done
import akka.stream.scaladsl.{Sink, Source}
import com.sksamuel.pulsar4s.{Consumer, ConsumerMessage, MessageId, Producer, ProducerMessage, Topic}

import scala.concurrent.Future

package object streams {

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
   * Create an Akka Streams source for the given [[Consumer]] that produces [[CommittableMessage]]s, which can be
   * acknowledged individually.
   *
   * @param create a function to create a new [[Consumer]].
   * @param seek an optional [[MessageId]] to seek to. Note that seeking will not work on multi-topic subscriptions.
   *             Prefer setting `subscriptionInitialPosition` in `ConsumerConfig` instead if you need to start at the
   *             earliest or latest offset.
   * @return the new [[Source]].
   */
  def committableSource[T](
    create: () => Consumer[T],
    seek: Option[MessageId] = None
  ): Source[CommittableMessage[T], Control] = {
    Source.fromGraph(new PulsarCommittableSourceGraphStage[T](create, seek))
  }

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
