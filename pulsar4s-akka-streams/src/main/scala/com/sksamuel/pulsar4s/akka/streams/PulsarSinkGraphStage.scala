package com.sksamuel.pulsar4s.akka.streams

import akka.stream.stage.{AsyncCallback, GraphStage, GraphStageLogic, InHandler}
import akka.stream.{Attributes, Inlet, SinkShape}
import com.sksamuel.exts.Logging
import com.sksamuel.pulsar4s.Producer

import scala.concurrent.ExecutionContextExecutor
import scala.util.{Failure, Success}

class PulsarSinkGraphStage[T](create: () => Producer[T]) extends GraphStage[SinkShape[T]] with Logging {

  private val in = Inlet.create[T]("pulsar.in")
  override def shape: SinkShape[T] = SinkShape.of(in)

  override def createLogic(inheritedAttributes: Attributes): GraphStageLogic = {
    new GraphStageLogic(shape) with InHandler {
      setHandler(in, this)

      implicit def context: ExecutionContextExecutor = super.materializer.executionContext

      val producer = create()
      var next: AsyncCallback[T] = _
      var error: Throwable = _

      override def preStart(): Unit = {
        next = getAsyncCallback { _ => pull(in) }
        pull(in)
      }

      override def onPush(): Unit = {
        try {
          val t = grab(in)
          logger.debug(s"Sending message $t")
          producer.sendAsync(t).onComplete {
            case Success(_) => next.invoke(t)
            case Failure(e) =>
              logger.error("Failing pulsar sink stage", e)
              failStage(e)
          }
        } catch {
          case e: Throwable =>
            logger.error("Failing pulsar sink stage", e)
            failStage(e)
        }
      }

      override def postStop(): Unit = {
        logger.debug("Graph stage stopping; closing producer")
        producer.close()
      }
    }
  }
}
