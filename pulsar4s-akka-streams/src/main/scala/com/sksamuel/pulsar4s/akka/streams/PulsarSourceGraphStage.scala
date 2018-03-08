package com.sksamuel.pulsar4s.akka.streams

import akka.stream.stage.{GraphStageLogic, GraphStageWithMaterializedValue, OutHandler}
import akka.stream.{Attributes, Outlet, SourceShape}
import com.sksamuel.pulsar4s.{Consumer, Message}

import scala.concurrent.ExecutionContext
import scala.util.{Failure, Success}

trait Control {
  def close(): Unit
}

class PulsarSourceGraphStage(create: () => Consumer) extends GraphStageWithMaterializedValue[SourceShape[Message], Control] {

  private val out = Outlet[Message]("pulsar.out")
  override def shape: SourceShape[Message] = SourceShape(out)

  override def createLogicAndMaterializedValue(inheritedAttributes: Attributes): (GraphStageLogic, Control) = {
    val logic = new GraphStageLogic(shape) with OutHandler {
      setHandler(out, this)

      implicit val context: ExecutionContext = super.materializer.executionContext
      val consumer = create()

      override def onPull(): Unit = {
        consumer.receiveAsync.onComplete {
          case Success(msg) => push(out, msg)
          case Failure(t) => failStage(t)
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
