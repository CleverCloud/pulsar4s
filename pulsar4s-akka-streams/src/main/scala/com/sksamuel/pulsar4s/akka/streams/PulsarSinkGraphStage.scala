package com.sksamuel.pulsar4s.akka.streams

import akka.stream.stage.{GraphStage, GraphStageLogic, InHandler}
import akka.stream.{Attributes, Inlet, SinkShape}
import com.sksamuel.pulsar4s.{Message, Producer}

import scala.concurrent.ExecutionContextExecutor
import scala.util.{Failure, Success}

class PulsarSinkGraphStage(create: () => Producer) extends GraphStage[SinkShape[Message]] {

  private val in = Inlet.create[Message]("pulsar.in")
  override def shape: SinkShape[Message] = SinkShape.of(in)

  override def createLogic(inheritedAttributes: Attributes): GraphStageLogic = {
    new GraphStageLogic(shape) with InHandler {
      setHandler(in, this)

      implicit def context: ExecutionContextExecutor = super.materializer.executionContext

      val producer = create()
      override def onPush(): Unit = {
        try {
          val msg = grab(in)
          producer.sendAsync(msg).onComplete {
            case Success(_) => pull(in)
            case Failure(t) => failStage(t)
          }
        } catch {
          case t: Throwable => failStage(t)
        }
      }

      override def preStart(): Unit = pull(in)
      override def postStop(): Unit = producer.close()
      override def onUpstreamFailure(t: Throwable): Unit = producer.close()
    }
  }
}
