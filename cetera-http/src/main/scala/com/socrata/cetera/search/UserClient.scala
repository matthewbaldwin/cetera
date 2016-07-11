package com.socrata.cetera.search

import com.rojoma.json.v3.util.JsonUtil
import org.elasticsearch.index.query.QueryBuilders
import org.slf4j.LoggerFactory

import com.socrata.cetera._
import com.socrata.cetera.types._
import com.socrata.cetera.util.{JsonDecodeException, LogHelper}

trait BaseUserClient {
  def fetch(id: String): Option[EsUser]
  def search(q: Option[String], limit: Int, offset: Int): (Seq[EsUser], Long)
}

class UserClient(esClient: ElasticSearchClient, indexAliasName: String) extends BaseUserClient {
  val logger = LoggerFactory.getLogger(getClass)

  def fetch(id: String): Option[EsUser] = {
    val get = esClient.client.prepareGet(indexAliasName, esUserType, id)
    logger.info("Elasticsearch request: " + get.request.toString)

    val res = get.execute.actionGet
    EsUser(res.getSourceAsString)
  }

  // TODO: setup user search pagination
  val maxLimit = 200 // Parity with Core for now
  def search(query: Option[String], limit: Int = maxLimit, offset: Int = 0): (Seq[EsUser], Long) = {
    val baseQuery = query match {
      case None => QueryBuilders.matchAllQuery()
      case Some(q) =>
        QueryBuilders
          .queryStringQuery(q)
          .field(ScreenName.fieldName)
          .field(ScreenName.rawFieldName)
          .field(Email.fieldName)
          .field(Email.rawFieldName)
          .autoGeneratePhraseQueries(true)
    }
    val req = esClient.client.prepareSearch(indexAliasName)
      .setTypes(esUserType)
      .setQuery(baseQuery)
      .setFrom(offset)
      .setSize(limit)
    logger.info(LogHelper.formatEsRequest(req))

    val res = req.execute.actionGet
    val timing = res.getTookInMillis

    val users = res.getHits.hits.flatMap { hit =>
      try { EsUser(hit.sourceAsString) }
      catch { case e: Exception =>
        logger.info(e.getMessage)
        None
      }
    }

    (users, timing)
  }
}
