package com.sksamuel.pulsar4s.jackson

import com.sksamuel.pulsar4s.{MessageReader, MessageWriter, Message}

object Jackson {

  implicit def writer[T: Manifest]: MessageWriter[T] = new MessageWriter[T] {
    override def write(t: T): Nothing = ???
  }

  implicit def reader[T: Manifest]: MessageReader[T] = new MessageReader[T] {
    override def read(msg: Message): T = JacksonSupport.mapper.readValue[T](msg.data)
  }

}
