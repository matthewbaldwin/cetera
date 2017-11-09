package com.socrata.cetera.search

import scala.collection.JavaConverters._

import org.elasticsearch.action.search.SearchRequestBuilder
import org.elasticsearch.common.text.Text
import org.elasticsearch.search.aggregations.bucket.terms.Terms
import org.elasticsearch.search.SearchHits
import org.elasticsearch.search.fetch.subphase.highlight.HighlightBuilder
import org.elasticsearch.search.fetch.subphase.highlight.SearchContextHighlight.FieldOptions
import org.slf4j.LoggerFactory

import com.socrata.cetera._
import com.socrata.cetera.auth.AuthedUser
import com.socrata.cetera.handlers.{PagingParamSet, UserScoringParamSet, UserSearchParamSet}
import com.socrata.cetera.response.{CompletionMatch, MatchSpan, UserCompletionResult}
import com.socrata.cetera.search.UserQueries.userQuery
import com.socrata.cetera.types._
import com.socrata.cetera.util.LogHelper


class UserClient(esClient: ElasticSearchClient, indexAliasName: String) {
  val logger = LoggerFactory.getLogger(getClass)

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
          .setQuery(DocumentQuery().domainIdQuery(Set(d.id)))
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
      domainForRoles: Domain,
      authorizedUser: AuthedUser)
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
      .addSort(Sorts.buildSort(pagingParams.sortKey, pagingParams.sortOrder, Some(domainForRoles.id)))
    logger.info(LogHelper.formatEsRequest(req))

    val res = req.execute.actionGet
    val timing = res.getTookInMillis

    val totalHits = res.getHits.getTotalHits()
    val users = res.getHits.getHits.flatMap { hit =>
      try { Some(EsUser.fromSource(hit.getSourceAsString)) }
      catch { case e: Exception =>
        logger.info(e.getMessage)
        None
      }
    }

    (users, totalHits, timing)
  }

  private def completionResultsFromSearchHits(domainForRoles: Domain, hits: SearchHits): List[UserCompletionResult] = {
    hits.getHits.toList.flatMap { hit =>
      try {
        val matchedFields = hit.getMatchedQueries.toList.collect {
          case field: String if (field == UserScreenName.fieldName) => UserScreenName
          case field: String if (field == UserEmail.fieldName) => UserEmail
        }

        val highlightMap = {
          val highlightFields = hit.getHighlightFields.asScala
          List(UserScreenName, UserEmail).flatMap { field =>
            highlightFields.get(field.autocompleteFieldName).map(value => (field, value))
          }.toMap
        }

        val matches = matchedFields.map { field =>
          val markedUpFieldValue = highlightMap(field)
          val highlightedStrings = markedUpFieldValue.fragments.collect { case t: Text => t.toString }.toList
          CompletionMatch(field.fieldName, highlightedStrings.flatMap(MatchSpan.fromHighlightedString))
        }

        val user = DomainUser(domainForRoles, EsUser.fromSource(hit.getSourceAsString))

        Some(UserCompletionResult(matches, user))
      } catch { case e: Exception =>
        logger.info(e.getMessage)
        None
      }
    }
  }

  def suggest(
      searchParams: UserSearchParamSet,
      scoringParams: UserScoringParamSet,
      pagingParams: PagingParamSet,
      domain: Option[Domain],
      domainForRoles: Domain,
      authorizedUser: AuthedUser)
    : (List[UserCompletionResult], Long, Long) = {

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

    val highlighterOptions = new FieldOptions()
      .preTags()

    val highlighter = new HighlightBuilder()
      .field(UserEmail.autocompleteFieldName)
      .field(UserScreenName.autocompleteFieldName)
      .highlighterType("fvh")
      .preTags("<span class=highlight>")
      .postTags("</span>")

    val req = esClient.client.prepareSearch(indexAliasName)
      .setTypes(esUserType)
      .setQuery(UserQueries.autocompleteQuery(searchParams, scoringParams, domain, authorizedUser))
      .setFrom(pagingParams.offset)
      .setSize(pagingParams.limit)
      .addSort(Sorts.buildSort(pagingParams.sortKey, pagingParams.sortOrder, Some(domainForRoles.id)))
      .highlighter(highlighter)

    logger.info(LogHelper.formatEsRequest(req))

    val res = req.execute.actionGet
    val timing = res.getTookInMillis
    val totalHits = res.getHits.getTotalHits()

    (completionResultsFromSearchHits(domainForRoles, res.getHits), totalHits, timing)
  }

}
