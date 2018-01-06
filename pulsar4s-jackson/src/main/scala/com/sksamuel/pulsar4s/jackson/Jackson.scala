package com.sksamuel.pulsar4s.jackson

import com.sksamuel.pulsar4s.{MessageReader, MessageWriter, SMessage}

object Jackson {

  implicit def writer[T: Manifest]: MessageWriter[T] = new MessageWriter[T] {
    override def write(t: T): Nothing = ???
  }

  implicit def reader[T: Manifest]: MessageReader[T] = new MessageReader[T] {
    override def read(msg: SMessage): T = JacksonSupport.mapper.readValue[T](msg.data)
  }

}
