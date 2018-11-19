package com.sksamuel.pulsar4s.akka.streams

import java.io.Closeable

import akka.stream.stage.{AsyncCallback, GraphStageLogic, GraphStageWithMaterializedValue, OutHandler}
import akka.stream.{Attributes, Outlet, SourceShape}
import com.sksamuel.exts.Logging
import com.sksamuel.pulsar4s.{Consumer, ConsumerMessage, MessageId}

import scala.concurrent.ExecutionContext
import scala.util.{Failure, Success}

trait Control extends Closeable {
  def close(): Unit
}

class PulsarSourceGraphStage[T](create: () => Consumer[T], seek: Option[MessageId])
  extends GraphStageWithMaterializedValue[SourceShape[ConsumerMessage[T]], Control]
    with Logging {

  private val out = Outlet[ConsumerMessage[T]]("pulsar.out")
  override def shape: SourceShape[ConsumerMessage[T]] = SourceShape(out)

  override def createLogicAndMaterializedValue(inheritedAttributes: Attributes): (GraphStageLogic, Control) = {

    val logic: GraphStageLogic = new GraphStageLogic(shape) with OutHandler {
      setHandler(out, this)

      var consumer: Consumer[T] = _
      var callback: AsyncCallback[ConsumerMessage[T]] = _

      override def preStart(): Unit = {
        consumer = create()
        seek foreach consumer.seek
        callback = getAsyncCallback(msg => push(out, msg))
      }

      override def onPull(): Unit = {
        implicit val context: ExecutionContext = super.materializer.executionContext
        logger.debug("Pull received; asking consumer for message")
        consumer.receiveAsync.onComplete {
          case Success(msg) =>
            logger.debug(s"Msg received $msg")
            callback.invoke(msg)
            consumer.acknowledge(msg.messageId)
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
