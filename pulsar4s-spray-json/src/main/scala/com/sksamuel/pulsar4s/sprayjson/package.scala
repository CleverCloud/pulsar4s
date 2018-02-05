package com.sksamuel.pulsar4s

import java.nio.charset.Charset

import scala.annotation.implicitNotFound
import scala.util.Try

package object sprayjson {

  import spray.json._

  @implicitNotFound("No RootJsonWriter for type ${T} found. Bring an implicit RootJsonWriter[T] instance in scope")
  implicit def sprayJsonWriter[T](implicit w: RootJsonWriter[T]): MessageWriter[T] = new MessageWriter[T] {
    override def write(t: T): Try[Message] = {
      Try {
        val bytes =w.write(t).compactPrint.getBytes(Charset.forName("UTF-8"))
        Message(None, bytes, Map.empty, None, 0, System.currentTimeMillis())
      }
    }
  }

  @implicitNotFound("No RootJsonReader for type ${T} found. Bring an implicit RootJsonReader[T] instance in scope")
  implicit def sprayJsonReader[T](implicit r: RootJsonReader[T]): MessageReader[T] = new MessageReader[T] {
    override def read(msg: Message): Try[T] = Try {
      r.read(new String(msg.data, "UTF-8").parseJson)
    }
  }
}
