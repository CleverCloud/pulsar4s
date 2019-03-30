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

trait Control {

  /**
   * Stop producing messages from the Pulsar consumer `Source` without shutting down the consumer.
   */
  def stop(): Unit

  /**
   * Shut down the Pulsar consumer, and shut down the `Source` if it is not already shut down.
   */
  def shutdown()(implicit ec: ExecutionContext): Future[Done]

  /**
   * Stop producing messages from the `Source`, wait for stream completion and shut down the consumer `Source` so that
   * all consumed messages reach the end of the stream. Failures in stream completion will be propagated.
   */
  def drainAndShutdown[S](streamCompletion: Future[S])(implicit ec: ExecutionContext): Future[S] = {
    stop()
    streamCompletion
      .recoverWith {
        case e: Throwable =>
          shutdown()
            .flatMap(_ => streamCompletion)
            .recoverWith {
              case _: Throwable => throw e
            }
      }
      .flatMap { result =>
        shutdown()
          .map(_ => result)
          .recover {
            case e: Throwable => throw e
          }
      }
  }

  /**
   * Get stats for the Pulsar consumer backing the `Source`.
   */
  def stats: ConsumerStats
}

class PulsarSourceGraphStage[T](create: () => Consumer[T], seek: Option[MessageId])
  extends GraphStageWithMaterializedValue[SourceShape[ConsumerMessage[T]], Control]
    with Logging {

  private val out = Outlet[ConsumerMessage[T]]("pulsar.out")
  override def shape: SourceShape[ConsumerMessage[T]] = SourceShape(out)

  override def createLogicAndMaterializedValue(inheritedAttributes: Attributes): (GraphStageLogic, Control) = {

    val logic: GraphStageLogic with Control = new GraphStageLogic(shape) with OutHandler with Control {
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

      override def stop(): Unit = completeStage()

      override def shutdown()(implicit ec: ExecutionContext): Future[Done] = {
        completeStage()
        consumer.closeAsync.map(_ => Done)
      }

      override def stats: ConsumerStats = consumer.stats
    }

    (logic, logic)
  }
}
