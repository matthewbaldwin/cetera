package com.socrata.cetera.search

import java.io.Closeable
import java.net.InetAddress

import org.elasticsearch.action.admin.indices.exists.indices.IndicesExistsRequest
import org.elasticsearch.client.Client
import org.elasticsearch.common.settings.Settings
import org.elasticsearch.common.transport.InetSocketTransportAddress
import org.elasticsearch.transport.client.PreBuiltTransportClient
import org.slf4j.LoggerFactory

import com.socrata.cetera.config.ElasticSearchConfig

class ElasticSearchClient(host: String, port: Int, clusterName: String, indexAliasName: String) extends Closeable {
  val logger = LoggerFactory.getLogger(getClass)

  val settings = Settings.builder()
    .put("cluster.name", clusterName)
    .put("client.transport.sniff", true)
    .build()

  val client: Client = new PreBuiltTransportClient(settings)
    .addTransportAddress(new InetSocketTransportAddress(InetAddress.getByName(host), port))

  def close(): Unit = client.close()

  def indexExists: Boolean = {
    val request = client.admin.indices.exists(new IndicesExistsRequest(indexAliasName))
    request.actionGet.isExists
  }
}

object ElasticSearchClient {
  def apply(config: ElasticSearchConfig): ElasticSearchClient =
    new ElasticSearchClient(config.elasticSearchServer,
      config.elasticSearchPort,
      config.elasticSearchClusterName,
      config.indexAliasName)
}
