package com.sksamuel.pulsar4s

case class PulsarTopic(mode: String, tenant: String, namespace: String, topic: String)

object PulsarTopic {

  private val Regex = "(.*?://)?(.*?)/(.*?)/(.*?)".r

  def unapply(str: String): Option[(String, String, String, String)] = {
    str match {
      case Regex(mode, tenant, namespace, topic) => Some(mode, tenant, namespace, topic)
      case Regex(tenant, namespace, topic) => Some("persistent", tenant, namespace, topic)
      case _ => None
    }
  }
}