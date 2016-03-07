package com.socrata.cetera.search

import com.rojoma.json.v3.codec.JsonDecode
import com.rojoma.json.v3.io.JsonReader
import org.elasticsearch.action.search.{SearchRequestBuilder, SearchType}
import org.elasticsearch.index.query.QueryBuilders
import org.slf4j.LoggerFactory

import com.socrata.cetera._
import com.socrata.cetera.search.DomainAggregations._
import com.socrata.cetera.search.DomainFilters._
import com.socrata.cetera.types.{Domain, DomainCnameFieldType, QueryType}
import com.socrata.cetera.util.LogHelper

class DomainClient(val esClient: ElasticSearchClient, val indexAliasName: String) {
  val logger = LoggerFactory.getLogger(getClass)

  def fetch(id: Int): Option[Domain] = {
    val res = esClient.client.prepareGet(indexAliasName, esDomainType, id.toString)
      .execute.actionGet
    Domain(res.getSourceAsString)
  }

  def find(cname: String): Option[Domain] = {
    val search = esClient.client.prepareSearch(indexAliasName).setTypes(esDomainType)
      .setQuery(QueryBuilders.matchPhraseQuery(DomainCnameFieldType.fieldName, cname))
    logger.debug(LogHelper.formatEsRequest(search))
    val res = search.execute.actionGet
    val hits = res.getHits.hits
    hits.length match {
      case 0 => None
      case n: Int =>
        val domainAsJvalue = JsonReader.fromString(hits(0).getSourceAsString)
        val domainDecode = JsonDecode.fromJValue[Domain](domainAsJvalue)
        domainDecode match {
          case Right(domain) => Some(domain)
          case Left(err) =>
            logger.error(err.english)
            throw new Exception(s"Error decoding $esDomainType $cname")
        }
    }
  }

  def odnSearch: Seq[Domain] = {
    val search = esClient.client.prepareSearch(indexAliasName).setTypes(esDomainType)
      .setQuery(QueryBuilders.filteredQuery(QueryBuilders.matchAllQuery(), isCustomerDomainFilter))
    logger.debug(LogHelper.formatEsRequest(search))
    val res = search.execute.actionGet
    res.getHits.hits.flatMap { h =>
      Domain(h.getSourceAsString)
    }
  }

  def buildCountRequest(
      searchQuery: QueryType,
      domainCnames: Set[String],
      searchContext: Option[Domain],
      categories: Option[Set[String]],
      tags: Option[Set[String]],
      only: Option[Seq[String]])
    : SearchRequestBuilder = {
    val (domainsFilter, domainsAggregation) = {
      val contextMod = searchContext.exists(_.moderationEnabled)
      val ds = searchContext.toSet ++ domainCnames.flatMap(find)
      val dsFilter = if (ds.nonEmpty) domainIds(ds.map(_.domainId)) else isCustomerDomainFilter
      val dsAggregation =
        if (ds.nonEmpty) {
          val domainIdsModerated = ds.filter(_.moderationEnabled).map(_.domainId)
          val domainIdsUnmoderated = ds.filterNot(_.moderationEnabled).map(_.domainId)
          domains(contextMod, domainIdsModerated, domainIdsUnmoderated)
        } else {
          val customerDomains = odnSearch
          val customerDomainIdsModerated = customerDomains.filter(_.moderationEnabled).map(_.domainId).toSet
          val customerDomainIdsUnmoderated = customerDomains.filterNot(_.moderationEnabled).map(_.domainId).toSet
          domains(contextMod, customerDomainIdsModerated, customerDomainIdsUnmoderated)
        }
      (dsFilter, dsAggregation)
    }
    esClient.client.prepareSearch(indexAliasName).setTypes(esDomainType)
      .setQuery(QueryBuilders.filteredQuery(QueryBuilders.matchAllQuery(), domainsFilter))
      .addAggregation(domainsAggregation)
      .setSearchType(SearchType.COUNT)
  }
}

class DomainNotFound(cname: String) extends NoSuchElementException(cname)
