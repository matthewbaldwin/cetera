package com.socrata.cetera.search

import org.slf4j.LoggerFactory

import com.socrata.cetera._
import com.socrata.cetera.auth.AuthedUser
import com.socrata.cetera.handlers.{PagingParamSet, UserSearchParamSet}
import com.socrata.cetera.search.UserQueries.userQuery
import com.socrata.cetera.types._
import com.socrata.cetera.util.LogHelper

class UserClient(esClient: ElasticSearchClient, indexAliasName: String) {
  val logger = LoggerFactory.getLogger(getClass)

  def fetch(id: String): Option[EsUser] = {
    val get = esClient.client.prepareGet(indexAliasName, esUserType, id)
    logger.info("Elasticsearch request: " + get.request.toString)

    val res = get.execute.actionGet
    EsUser(res.getSourceAsString)
  }

  def search(
      searchParams: UserSearchParamSet,
      pagingParams: PagingParamSet,
      domain: Option[Domain],
      authorizedUser: Option[AuthedUser])
    : (Seq[EsUser], Long, Long) = {

    val req = esClient.client.prepareSearch(indexAliasName)
      .setTypes(esUserType)
      .setQuery(userQuery(searchParams, domain, authorizedUser))
      .setFrom(pagingParams.offset)
      .setSize(pagingParams.limit)
    logger.info(LogHelper.formatEsRequest(req))

    val res = req.execute.actionGet
    val timing = res.getTookInMillis

    val totalHits = res.getHits.totalHits()
    val users = res.getHits.hits.flatMap { hit =>
      try { EsUser(hit.sourceAsString) }
      catch { case e: Exception =>
        logger.info(e.getMessage)
        None
      }
    }

    (users, totalHits, timing)
  }
}
