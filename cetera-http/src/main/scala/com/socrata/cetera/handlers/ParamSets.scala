package com.socrata.cetera.handlers

import org.joda.time.DateTime

import com.socrata.cetera.types._

case class SearchParamSet(
    searchQuery: QueryType = NoQuery,
    domains: Option[Set[String]] = None,
    searchContext: Option[String] = None,
    domainMetadata: Option[Set[(String, String)]] = None,
    categories: Option[Set[String]] = None,
    tags: Option[Set[String]] = None,
    datatypes: Option[Set[String]] = None,
    user: Option[String] = None,
    sharedTo: Option[String] = None,
    attribution: Option[String] = None,
    provenance: Option[String] = None,
    parentDatasetId: Option[String] = None,
    ids: Option[Set[String]] = None,
    public: Option[Boolean] = None,
    published: Option[Boolean] = None,
    derived: Option[Boolean] = None,
    explicitlyHidden: Option[Boolean] = None,
    approvalStatus: Option[ApprovalStatus] = None,
    license: Option[String] = None)

case class AgeDecayParamSet(
    decayType: String,
    scale: String,
    decay: Double,
    offset: String,
    origin: DateTime = new DateTime())

case class ScoringParamSet(
    fieldBoosts: Map[CeteraFieldType with Boostable, Float] = Map.empty,
    datatypeBoosts: Map[Datatype, Float] = Map.empty,
    domainBoosts: Map[String, Float] = Map.empty,
    officialBoost: Float = 1.toFloat,
    minShouldMatch: Option[String] = None,
    slop: Option[Int] = None,
    ageDecay: Option[AgeDecayParamSet] = None)

case class PagingParamSet(
    offset: Int = PagingParamSet.defaultPageOffset,
    limit: Int = PagingParamSet.defaultPageLength,
    sortOrder: Option[String] = None)

object PagingParamSet {
  val defaultPageOffset = 0
  val defaultPageLength = 100
}

case class FormatParamSet(
    showScore: Boolean = false,
    showVisibility: Boolean = false,
    locale: Option[String] = None)

case class UserSearchParamSet(
    ids: Option[Set[String]] = None,
    emails: Option[Set[String]] = None,
    screenNames: Option[Set[String]] = None,
    flags: Option[Set[String]] = None,
    roles: Option[Set[String]] = None,
    domain: Option[String] = None,
    query: Option[String] = None)
