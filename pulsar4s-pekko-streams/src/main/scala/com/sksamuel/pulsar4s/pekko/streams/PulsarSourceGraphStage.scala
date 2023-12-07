package com.sksamuel.pulsar4s.pekko.streams

import org.apache.pekko.Done
import org.apache.pekko.annotation.DoNotInherit
import org.apache.pekko.stream.Attributes
import org.apache.pekko.stream.Outlet
import org.apache.pekko.stream.SourceShape
import org.apache.pekko.stream.stage.AsyncCallback
import org.apache.pekko.stream.stage.GraphStageLogic
import org.apache.pekko.stream.stage.GraphStageWithMaterializedValue
import org.apache.pekko.stream.stage.OutHandler
import com.sksamuel.exts.Logging
import com.sksamuel.pulsar4s.Consumer
import com.sksamuel.pulsar4s.ConsumerMessage
import com.sksamuel.pulsar4s.MessageId
import org.apache.pulsar.client.api.ConsumerStats

import scala.concurrent.Await
import scala.concurrent.duration.Duration
import scala.concurrent.ExecutionContext
import scala.concurrent.Future
import scala.concurrent.Promise
import scala.util.Failure
import scala.util.Success
import scala.util.Try
import scala.util.control.NonFatal

/**
  * Provides operations for controlling the Pulsar Pekko streams sources.
  *
  * This trait is not meant to be inherited by external code.
  */
@DoNotInherit
trait Control {

  /**
   * Complete the source but leave the Pulsar consumer open to receive any unacked messages.
   *
   * @return a future completed with `Done` when the stream is completed.
   */
  def complete()(implicit ec: ExecutionContext): Future[Done]

  /**
   * Complete the source but leave the Pulsar consumer open to receive any unacked messages.
   */
  @deprecated("This method is blocking. Use `complete` instead.", "2.7.1")
  def stop(): Unit = {
    Await.result(complete()(ExecutionContext.global), Duration.Inf)
    ()
  }

  /**
   * Shut down the Pulsar consumer, and shut down the `Source` if it is not already shut down.
   *
   * @return a future completed with `Done` when the source is shut down and the consumer is closed.
   */
  def shutdown()(implicit ec: ExecutionContext): Future[Done]

  /**
   * Stop producing messages from the `Source`, wait for stream completion and shut down the consumer `Source` so that
   * all consumed messages reach the end of the stream. Failures in stream completion will be propagated.
   */
  def drainAndShutdown[S](streamCompletion: Future[S])(implicit ec: ExecutionContext): Future[S] = {
    complete()
      .flatMap(_ => streamCompletion)
      .transformWith { resultTry =>
        shutdown().transform {
          case Success(_) => resultTry
          case Failure(e) => resultTry.flatMap(_ => Failure(e))
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
            
      implicit def ec: ExecutionContext = materializer.executionContext

      @inline private def consumer: Consumer[T] =
        consumerOpt.getOrElse(throw new IllegalStateException("Consumer not initialized!"))
      private var consumerOpt: Option[Consumer[T]] = None
      private val receiveCallback: AsyncCallback[Try[ConsumerMessage[T]]] = getAsyncCallback {
        case Success(msg) =>
          logger.debug(s"Msg received $msg")
          push(out, msg)
          consumer.acknowledge(msg.messageId)
        case Failure(e) =>
          logger.warn("Error when receiving message", e)
          failStage(e)
      }
      private val stopped: Promise[Done] = Promise()
      private val stopCallback: AsyncCallback[Unit] = getAsyncCallback { _ => completeStage() }

      override def preStart(): Unit = {
        try {
          val consumer = create()
          consumerOpt = Some(consumer)
          stopped.future.onComplete { _ =>
            // Note: unlike the committable source, we don't expect acks so we can close immediately
            close()
          }
          seek foreach consumer.seek
        } catch {
          case NonFatal(e) =>
            logger.error("Error creating consumer!", e)
            failStage(e)
        }
      }

      override def onPull(): Unit = {
        logger.debug("Pull received; asking consumer for message")
        consumer.receiveAsync.onComplete(receiveCallback.invoke(_))
      }

      override def postStop(): Unit = stopped.success(Done)

      override def complete()(implicit ec: ExecutionContext): Future[Done] = {
        stopCallback.invoke(())
        stopped.future
      }

      private def close()(implicit ec: ExecutionContext): Future[Done] =
        consumerOpt.fold(Future.successful(Done))(_.closeAsync.map(_ => Done))

      override def shutdown()(implicit ec: ExecutionContext): Future[Done] =
        for {
          _ <- complete()
          _ <- close()
        } yield Done

      override def stats: ConsumerStats = consumer.stats
    }

    (logic, logic)
  }
}
