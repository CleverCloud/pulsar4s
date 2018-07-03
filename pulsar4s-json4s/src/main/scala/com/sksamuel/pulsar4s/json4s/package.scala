package com.sksamuel.pulsar4s

import org.apache.pulsar.client.api.Schema

import scala.reflect.Manifest

package object json4s {

  import org.json4s._

  implicit def schema[T <: AnyRef : Manifest](implicit serialization: Serialization, formats: Formats): Schema[T] = new Schema[T] {
    override def encode(t: T): Array[Byte] = serialization.write(t).getBytes("UTF-8")
    override def decode(bytes: Array[Byte]): T = serialization.read[T](new String(bytes, "UTF-8"))
    override def getSchemaInfo: org.apache.pulsar.shade.org.apache.pulsar.common.schema.SchemaInfo = {
      val info = new org.apache.pulsar.shade.org.apache.pulsar.common.schema.SchemaInfo()
      info.setName(manifest[T].runtimeClass.getCanonicalName)
      info.setType(org.apache.pulsar.shade.org.apache.pulsar.common.schema.SchemaType.JSON)
      info
    }
  }
}
