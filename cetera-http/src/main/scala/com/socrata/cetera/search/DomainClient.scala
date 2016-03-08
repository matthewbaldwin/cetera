package com.socrata.cetera.search

import com.rojoma.json.v3.codec.JsonDecode
import com.rojoma.json.v3.io.JsonReader
import org.elasticsearch.action.search.SearchRequestBuilder
import org.elasticsearch.index.query.QueryBuilders
import org.slf4j.LoggerFactory

import com.socrata.cetera._
import com.socrata.cetera.types.{Domain, DomainCnameFieldType, QueryType}
import com.socrata.cetera.util.{JsonDecodeException, LogHelper}

class DomainClient(val esClient: ElasticSearchClient, val indexAliasName: String) {
  val logger = LoggerFactory.getLogger(getClass)

  def fetch(id: Int): Option[Domain] = fetch(id.toString)
  def fetch(id: String): Option[Domain] = {
    val res = esClient.client.prepareGet(indexAliasName, esDomainType, id)
      .execute.actionGet
    Domain(res.getSourceAsString)
  }

  def find(cname: String): Option[Domain] = {
    val search = esClient.client.prepareSearch(indexAliasName).setTypes(esDomainType)
      .setQuery(QueryBuilders.matchPhraseQuery(DomainCnameFieldType.fieldName, cname))
    logger.info(LogHelper.formatEsRequest(search))
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

  def buildCountRequest(
      searchQuery: QueryType,
      domains: Set[String],
      searchContext: Option[Domain],
      categories: Option[Set[String]],
      tags: Option[Set[String]],
      only: Option[Seq[String]])
    : SearchRequestBuilder = {
    esClient.client.prepareSearch(indexAliasName).setTypes(esDomainType)
      .addAggregation(Aggregations.domains)
      .setSearchType("count")
  }
}
