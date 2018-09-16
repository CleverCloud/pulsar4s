package com.sksamuel.pulsar4s.akka

import akka.NotUsed
import akka.stream.scaladsl.{Sink, Source}
import com.sksamuel.pulsar4s.{Consumer, Message, MessageId, Producer}

package object streams {
  def source[T](create: () => Consumer[T], seek: MessageId): Source[Message[T], Control] = Source.fromGraph(new PulsarSourceGraphStage(create, seek))
  def sink[T](create: () => Producer[T]): Sink[T, NotUsed] = Sink.fromGraph(new PulsarSinkGraphStage(create))
}
