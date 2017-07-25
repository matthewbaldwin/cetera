package com.socrata.cetera.search

import scala.collection.JavaConverters._
import org.slf4j.LoggerFactory

import org.elasticsearch.search.aggregations.bucket.terms.Terms

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

  // NOTE: this should be bigger than number of users w/ assets on a given domain
  val OwnerAggregationSize = 10000

  def fetchDomainOwnerIds(domain: Option[Domain]): (Set[String], Long) = {
    // TODO: this isn't really the right place to put this validation for a few reasons:
    //   - if domain is not optional, it shouldn't be an Option
    //   - if domain is a required parameter, we should validate in the `QueryParametersParser`
    //
    // Leaving this for the moment since a larger refactor is required for handling multiple domains.
    domain match {
      case None =>
        throw new IllegalArgumentException("The 'domain' parameter must be specified when using the 'only' parameter")
      case Some(d) =>
        val req = esClient.client.prepareSearch(indexAliasName)
          .setTypes(esDocumentType)
          .setQuery(DocumentQuery().domainIdQuery(Set(d.domainId)))
          .addAggregation(DocumentAggregations.owners(OwnerAggregationSize))

        val res = req.execute.actionGet

        val ownerIds = res.getAggregations.get[Terms]("owners").getBuckets.asScala.map(
          _.getKey.toString
        ).toSet

        (ownerIds, res.getTookInMillis)
    }
  }

  def search(
      searchParams: UserSearchParamSet,
      pagingParams: PagingParamSet,
      domain: Option[Domain],
      authorizedUser: Option[AuthedUser])
    : (Seq[EsUser], Long, Long) = {

    // NOTE: as we add more user types, we will want to modularize this logic for determining the appropriate
    // query given the `userType`
    val fetchedIds = searchParams.userType.collect { case Owner =>
      fetchDomainOwnerIds(domain)
    }

    val ids = (searchParams.ids, fetchedIds) match {
      case (Some(ids), Some((idsWithAssets, _))) => Some(ids & idsWithAssets)
      case (Some(ids), None) => Some(ids)
      case (None, Some((idsWithAssets, _))) => Some(idsWithAssets)
      case (None, None) => None
    }

    val req = esClient.client.prepareSearch(indexAliasName)
      .setTypes(esUserType)
      .setQuery(userQuery(searchParams.copy(ids=ids), domain, authorizedUser))
      .setFrom(pagingParams.offset)
      .setSize(pagingParams.limit)
    logger.info(LogHelper.formatEsRequest(req))

    val res = req.execute.actionGet
    val timing = res.getTookInMillis

    val totalHits = res.getHits.getTotalHits()
    val users = res.getHits.getHits.flatMap { hit =>
      try { EsUser(hit.getSourceAsString) }
      catch { case e: Exception =>
        logger.info(e.getMessage)
        None
      }
    }

    (users, totalHits, timing)
  }
}
