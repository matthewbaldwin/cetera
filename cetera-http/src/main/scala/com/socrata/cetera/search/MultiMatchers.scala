package com.socrata.cetera.search

import org.elasticsearch.index.query.MultiMatchQueryBuilder.Type.{CROSS_FIELDS, PHRASE}
import org.elasticsearch.index.query.{MultiMatchQueryBuilder, QueryBuilder, QueryBuilders}

import com.socrata.cetera.auth.AuthedUser
import com.socrata.cetera.handlers.ScoringParamSet
import com.socrata.cetera.types.{FullTextSearchAnalyzedFieldType, FullTextSearchRawFieldType}
import com.socrata.cetera.types.{PrivateFullTextSearchAnalyzedFieldType, PrivateFullTextSearchRawFieldType}

object MultiMatchers {
  type MatchType = MultiMatchQueryBuilder.Type
  type MatchQuery = MultiMatchQueryBuilder

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
      user: AuthedUser)
  : QueryBuilder = {
    val privateMatch = privateMatchQuery(q, mmType, scoringParams)
    val privateFilter = DocumentQuery(forDomainSearch = false).privateMetadataUserRestrictionsQuery(user)
    val matchPrivateFieldsAndRespectPrivacy = QueryBuilders.boolQuery().must(privateMatch).filter(privateFilter)

    val matchPublicFields = publicMatchQuery(q, mmType, scoringParams)
    QueryBuilders.boolQuery()
      .should(matchPublicFields)
      .should(matchPrivateFieldsAndRespectPrivacy)
  }

  def buildQuery(q: String,  mmType: MatchType, scoringParams: ScoringParamSet, user: Option[AuthedUser])
  : QueryBuilder =
    user match {
      case None => publicMatchQuery(q, mmType, scoringParams)
      case Some(u) if u.isSuperAdmin => fullMatchQuery(q, mmType, scoringParams)
      case Some(u) => selectiveMatchQuery(q, mmType, scoringParams, u)
    }
}
