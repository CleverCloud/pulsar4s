package com.sksamuel.pulsar4s.akka

import akka.NotUsed
import akka.stream.scaladsl.{Sink, Source}
import com.sksamuel.pulsar4s.{Consumer, Message, Producer}

package object streams {
  def source(create: () => Consumer): Source[Message, Control] = Source.fromGraph(new PulsarSourceGraphStage(create))
  def sink(create: () => Producer): Sink[Message, NotUsed] = Sink.fromGraph(new PulsarSinkGraphStage(create))
}
