package com.sksamuel.pulsar4s

import org.apache.pulsar.client.api.Authentication

import scala.concurrent.duration.FiniteDuration

case class PulsarClientConfig(serviceUrl: String,
                              allowTlsInsecureConnection: Option[Boolean] = None,
                              authentication: Option[Authentication] = None,
                              connectionsPerBroker: Option[Int] = None,
                              enableTcpNoDelay: Option[Boolean] = None,
                              enableTlsHostnameVerification: Option[Boolean] = None,
                              listenerThreads: Option[Int] = None,
                              maxConcurrentLookupRequests: Option[Int] = None,
                              maxNumberOfRejectedRequestPerConnection: Option[Int] = None,
                              operationTimeout: Option[FiniteDuration] = None,
                              keepAliveInterval: Option[FiniteDuration] = None,
                              statsInterval: Option[FiniteDuration] = None,
                              maxLookupRequests: Option[Int] = None,
                              tlsTrustCertsFilePath: Option[String] = None,
                              ioThreads: Option[Int] = None,
                              enableTransaction: Option[Boolean] = None,
                              additionalProperties: Map[String, AnyRef] = Map.empty)
