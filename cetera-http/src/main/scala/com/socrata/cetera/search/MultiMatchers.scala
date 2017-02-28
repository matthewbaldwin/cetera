package com.socrata.cetera.search

import org.elasticsearch.index.query.{MultiMatchQueryBuilder, QueryBuilder, QueryBuilders}
import org.elasticsearch.index.query.MultiMatchQueryBuilder.Type.{CROSS_FIELDS, PHRASE}

import com.socrata.cetera.auth.User
import com.socrata.cetera.handlers.ScoringParamSet
import com.socrata.cetera.types.{FullTextSearchAnalyzedFieldType, FullTextSearchRawFieldType}
import com.socrata.cetera.types.{PrivateFullTextSearchAnalyzedFieldType, PrivateFullTextSearchRawFieldType}

object MultiMatchers {
  type MatchType = MultiMatchQueryBuilder.Type
  type MatchQuery = MultiMatchQueryBuilder

  val docFilters = DocumentFilters(forDomainSearch = false)

  private def refineMatch(query: MatchQuery, mmType: MatchType, scoringParams: ScoringParamSet): Unit =
    mmType match {
      case CROSS_FIELDS =>
        scoringParams.minShouldMatch.foreach(msm => query.minimumShouldMatch(msm))
      case PHRASE =>
        scoringParams.slop.foreach(s => query.slop(s))
        scoringParams.fieldBoosts.foreach { case (field, weight) => query.field(field.fieldName, weight)}
      case _ =>
    }

  private def buildMatchQuery(
      q: String,
      fields: List[String],
      mmType: MatchType,
      scoringParams: ScoringParamSet)
  : MatchQuery = {
    val query = QueryBuilders.multiMatchQuery(q).`type`(mmType)
    fields.foreach(query.field(_))
    refineMatch(query, mmType, scoringParams)
    query
  }

  private def publicMatchQuery(q: String, mmType: MatchType, scoringParams: ScoringParamSet): MatchQuery = {
    val fields = List(FullTextSearchAnalyzedFieldType.fieldName, FullTextSearchRawFieldType.fieldName)
    buildMatchQuery(q, fields, mmType, scoringParams)
  }

  private def privateMatchQuery(q: String, mmType: MatchType, scoringParams: ScoringParamSet): MatchQuery = {
    val fields = List(PrivateFullTextSearchAnalyzedFieldType.fieldName, PrivateFullTextSearchRawFieldType.fieldName)
    buildMatchQuery(q, fields, mmType, scoringParams)
  }

  private def fullMatchQuery(q: String, mmType: MatchType, scoringParams: ScoringParamSet): MatchQuery = {
    val fields = List(
      FullTextSearchAnalyzedFieldType.fieldName, FullTextSearchRawFieldType.fieldName,
      PrivateFullTextSearchAnalyzedFieldType.fieldName, PrivateFullTextSearchRawFieldType.fieldName
    )
    buildMatchQuery(q, fields, mmType, scoringParams)
  }

  private def selectiveMatchQuery(
      q: String,
      mmType: MatchType,
      scoringParams: ScoringParamSet,
      user: User)
  : QueryBuilder = {
    val privateMatch = privateMatchQuery(q, mmType, scoringParams)
    val privateFilter = docFilters.privateMetadataUserRestrictionsFilter(user)
    val matchPrivateFieldsAndRespectPrivacy = QueryBuilders.filteredQuery(privateMatch, privateFilter)
    val matchPublicFields = publicMatchQuery(q, mmType, scoringParams)
    QueryBuilders.boolQuery()
      .should(matchPublicFields)
      .should(matchPrivateFieldsAndRespectPrivacy)
  }

  def buildQuery(q: String,  mmType: MatchType, scoringParams: ScoringParamSet, user: Option[User])
  : QueryBuilder =
    user match {
      case None => publicMatchQuery(q, mmType, scoringParams)
      case Some(u) if u.isSuperAdmin => fullMatchQuery(q, mmType, scoringParams)
      case Some(u) => selectiveMatchQuery(q, mmType, scoringParams, u)
    }
}
