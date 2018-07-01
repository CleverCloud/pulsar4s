package com.sksamuel.pulsar4s

case class PulsarClientConfig(serviceUrl: String,
                              allowTlsInsecureConnection: Option[Boolean],
                              connectionsPerBroker: Option[Int],
                              enableTcpNoDelay: Option[Boolean],
                              enableTls: Option[Boolean],
                              enableTlsHostnameVerification: Option[Boolean],
                              listenerThreads: Option[Int],
                              maxConcurrentLookupRequests: Option[Int],
                              maxNumberOfRejectedRequestPerConnection: Option[Int],
                              tlsTrustCertsFilePath: Option[String],
                              ioThreads: Option[Int])