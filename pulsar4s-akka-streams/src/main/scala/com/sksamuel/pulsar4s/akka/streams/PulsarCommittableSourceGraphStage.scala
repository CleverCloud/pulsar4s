package com.sksamuel.pulsar4s.akka.streams

import akka.Done
import akka.stream.Attributes
import akka.stream.Outlet
import akka.stream.SourceShape
import akka.stream.stage.AsyncCallback
import akka.stream.stage.GraphStageLogic
import akka.stream.stage.GraphStageWithMaterializedValue
import akka.stream.stage.OutHandler
import com.sksamuel.exts.Logging
import com.sksamuel.pulsar4s.Consumer
import com.sksamuel.pulsar4s.ConsumerMessage
import com.sksamuel.pulsar4s.MessageId
import org.apache.pulsar.client.api.ConsumerStats

import scala.concurrent.ExecutionContext
import scala.concurrent.Future
import scala.util.Failure
import scala.util.Success

trait CommittableMessage[T] {
  def ack(cumulative: Boolean = false): Future[Done]
  def nack(): Future[Done]
  def message: ConsumerMessage[T]
}

class PulsarCommittableSourceGraphStage[T](create: () => Consumer[T], seek: Option[MessageId])
  extends GraphStageWithMaterializedValue[SourceShape[CommittableMessage[T]], Control]
    with Logging {

  private val out = Outlet[CommittableMessage[T]]("pulsar.out")
  override def shape: SourceShape[CommittableMessage[T]] = SourceShape(out)

  private class PulsarCommittableSourceLogic(shape: Shape) extends GraphStageLogic(shape) with OutHandler with Control {
    setHandler(out, this)

    var consumer: Consumer[T] = _
    var receiveCallback: AsyncCallback[CommittableMessage[T]] = _

    override def preStart(): Unit = {
      implicit val context: ExecutionContext = super.materializer.executionContext
      consumer = create()
      seek foreach consumer.seek
      receiveCallback = getAsyncCallback(push(out, _))
    }

    override def onPull(): Unit = {
      implicit val context: ExecutionContext = super.materializer.executionContext
      logger.debug("Pull received; asking consumer for message")

      consumer.receiveAsync.onComplete {
        case Success(msg) =>
          logger.debug(s"Message received: $msg")
          receiveCallback.invoke(new CommittableMessage[T] {
            override def message: ConsumerMessage[T] = msg
            override def ack(cumulative: Boolean): Future[Done] = {
              logger.debug(s"Acknowledging message: $msg")
              val ackFuture = if (cumulative) {
                consumer.acknowledgeCumulativeAsync(msg.messageId)
              } else {
                consumer.acknowledgeAsync(msg.messageId)
              }
              ackFuture.map(_ => Done)
            }
            override def nack(): Future[Done] = {
              logger.debug(s"Negatively acknowledging message: $msg")
              consumer.negativeAcknowledgeAsync(msg.messageId).map(_ => Done)
            }
          })
        case Failure(e) =>
          logger.warn("Error when receiving message", e)
          failStage(e)
      }
    }

    override def stop(): Unit = completeStage()

    override def shutdown()(implicit ec: ExecutionContext): Future[Done] = {
      completeStage()
      consumer.closeAsync.map(_ => Done)
    }

    def stats: ConsumerStats = consumer.stats
  }

  override def createLogicAndMaterializedValue(inheritedAttributes: Attributes): (GraphStageLogic, Control) = {
    val logic = new PulsarCommittableSourceLogic(shape)
    (logic, logic)
  }
}

