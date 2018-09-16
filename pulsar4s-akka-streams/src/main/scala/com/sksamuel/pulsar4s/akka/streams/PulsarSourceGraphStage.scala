package com.sksamuel.pulsar4s.akka.streams

import akka.stream.stage.{AsyncCallback, GraphStageLogic, GraphStageWithMaterializedValue, OutHandler}
import akka.stream.{Attributes, Outlet, SourceShape}
import com.sksamuel.exts.Logging
import com.sksamuel.pulsar4s.{Consumer, Message, MessageId}

import scala.concurrent.ExecutionContext
import scala.util.{Failure, Success}

trait Control {
  def close(): Unit
}

class PulsarSourceGraphStage[T](create: () => Consumer[T], seek: MessageId)
  extends GraphStageWithMaterializedValue[SourceShape[Message[T]], Control]
    with Logging {

  private val out = Outlet[Message[T]]("pulsar.out")
  override def shape: SourceShape[Message[T]] = SourceShape(out)

  override def createLogicAndMaterializedValue(inheritedAttributes: Attributes): (GraphStageLogic, Control) = {

    val logic: GraphStageLogic = new GraphStageLogic(shape) with OutHandler {
      setHandler(out, this)

      var consumer: Consumer[T] = _
      var callback: AsyncCallback[Message[T]] = _

      override def preStart(): Unit = {
        consumer = create()
        consumer.seek(seek)
        callback = getAsyncCallback(msg => push(out, msg))
      }

      override def onPull(): Unit = {
        implicit val context: ExecutionContext = super.materializer.executionContext
        logger.debug("Pull received; asking consumer for message")
        consumer.receiveAsync.onComplete {
          case Success(msg) =>
            logger.debug(s"Msg received $msg")
            callback.invoke(msg)
          case Failure(e) =>
            logger.warn("Error when receiving message", e)
            failStage(e)
        }
      }

      override def postStop(): Unit = consumer.close()
    }

    val control = new Control {
      override def close(): Unit = {
        logic.completeStage()
      }
    }

    (logic, control)
  }
}
