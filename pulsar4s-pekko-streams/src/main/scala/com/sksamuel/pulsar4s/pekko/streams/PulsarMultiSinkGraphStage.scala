package com.sksamuel.pulsar4s.pekko.streams

import org.apache.pekko.Done
import org.apache.pekko.stream.stage.{AsyncCallback, GraphStageLogic, GraphStageWithMaterializedValue, InHandler}
import org.apache.pekko.stream.{Attributes, Inlet, SinkShape}
import com.sksamuel.exts.Logging
import com.sksamuel.pulsar4s.{Producer, ProducerMessage, Topic}

import scala.concurrent.duration._
import scala.concurrent.{Await, ExecutionContextExecutor, Future, Promise}
import scala.util.{Failure, Success, Try}

class PulsarMultiSinkGraphStage[T](createFn: Topic => Producer[T], initTopics: Set[Topic] = Set.empty)
  extends GraphStageWithMaterializedValue[SinkShape[(Topic, ProducerMessage[T])], Future[Done]]
    with Logging {

  private val in = Inlet.create[(Topic, ProducerMessage[T])]("pulsar.in")

  override def shape: SinkShape[(Topic, ProducerMessage[T])] = SinkShape.of(in)

  override def createLogicAndMaterializedValue(inheritedAttributes: Attributes): (GraphStageLogic, Future[Done]) = {

    val promise = Promise[Done]()

    val logic: GraphStageLogic = new GraphStageLogic(shape) with InHandler {
      setHandler(in, this)

      implicit def context: ExecutionContextExecutor = super.materializer.executionContext

      var producers: Map[Topic, Producer[T]] = _
      var produceCallback: AsyncCallback[Try[_]] = _
      var error: Throwable = _

      override def preStart(): Unit = {
        producers = initTopics.map(t => t -> createFn(t)).toMap
        produceCallback = getAsyncCallback {
          case Success(_) => pull(in)
          case Failure(e) =>
            logger.error("Failing pulsar sink stage", e)
            failStage(e)
        }
        pull(in)
      }

      private def getProducer(topic: Topic): Producer[T] =
        producers.get(topic) match {
          case Some(p) => p
          case None =>
            logger.debug(s"creating new producer for topic $topic")
            val producer = createFn(topic)
            producers += topic -> producer
            producer
        }

      override def onPush(): Unit = {
        try {
          val (topic, message) = grab(in)
          logger.debug(s"Sending message $message to $topic")
          val producer = getProducer(topic)
          producer.sendAsync(message).onComplete(produceCallback.invoke)
        } catch {
          case e: Throwable =>
            logger.error("Failing pulsar sink stage", e)
            failStage(e)
        }
      }

      override def postStop(): Unit = {
        logger.debug("Graph stage stopping; closing producers")
        val fs = producers.flatMap { case (_, p) =>
          Seq(
            p.flushAsync,
            p.closeAsync
          )
        }
        Await.ready(Future.sequence(fs), 15.seconds)
      }

      override def onUpstreamFailure(ex: Throwable): Unit = {
        promise.tryFailure(ex)
      }

      override def onUpstreamFinish(): Unit = {
        promise.trySuccess(Done)
      }
    }

    (logic, promise.future)
  }

}
