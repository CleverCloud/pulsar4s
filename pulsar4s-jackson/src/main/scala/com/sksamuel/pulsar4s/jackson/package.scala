package com.sksamuel.pulsar4s

import org.apache.pulsar.client.api.Schema
import org.apache.pulsar.client.impl.schema.SchemaInfoImpl
import org.apache.pulsar.common.schema.{SchemaInfo, SchemaType}

package object jackson {
  implicit def schema[T: Manifest]: Schema[T] = new Schema[T] {
    override def clone(): Schema[T] = this
    override def encode(t: T): Array[Byte] = JacksonSupport.mapper.writeValueAsBytes(t)
    override def decode(bytes: Array[Byte]): T = JacksonSupport.mapper.readValue[T](bytes)
    override def getSchemaInfo: SchemaInfo =
      SchemaInfoImpl.builder()
        .name(manifest[T].runtimeClass.getCanonicalName)
        .`type`(SchemaType.JSON)
        .schema("""{"type":"any"}""".getBytes("UTF-8"))
        .build()
  }
}
