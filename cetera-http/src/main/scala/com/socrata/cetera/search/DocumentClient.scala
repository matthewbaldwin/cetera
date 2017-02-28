package com.socrata.cetera.search

import org.elasticsearch.action.search.SearchRequestBuilder
import org.elasticsearch.index.query.QueryBuilders
import org.elasticsearch.search.aggregations.AggregationBuilders

import com.socrata.cetera.auth.User
import com.socrata.cetera.esDocumentType
import com.socrata.cetera.handlers.{PagingParamSet, ScoringParamSet, SearchParamSet}
import com.socrata.cetera.errors.MissingRequiredParameterError
import com.socrata.cetera.search.DocumentAggregations.chooseAggregation
import com.socrata.cetera.search.DocumentQueries.{autocompleteQuery, chooseMatchQuery, compositeFilteredQuery}
import com.socrata.cetera.types._

trait BaseDocumentClient {
  def buildSearchRequest(
      domainSet: DomainSet,
      searchParams: SearchParamSet,
      scoringParams: ScoringParamSet,
      pagingParams: PagingParamSet,
      user: Option[User],
      requireAuth: Boolean)
    : SearchRequestBuilder

  def buildAutocompleteSearchRequest(
      domainSet: DomainSet,
      searchParams: SearchParamSet,
      scoringParams: ScoringParamSet,
      pagingParams: PagingParamSet,
      user: Option[User],
      requireAuth: Boolean)
    : SearchRequestBuilder

  def buildCountRequest(
      field: DocumentFieldType with Countable with Rawable,
      domainSet: DomainSet,
      searchParams: SearchParamSet,
      user: Option[User],
      requireAuth: Boolean)
    : SearchRequestBuilder

  def buildFacetRequest(
      domainSet: DomainSet,
      searchParams: SearchParamSet,
      user: Option[User],
      requireAuth: Boolean)
  : SearchRequestBuilder
}

class DocumentClient(
    esClient: ElasticSearchClient,
    domainClient: BaseDomainClient,
    indexAliasName: String,
    defaultTitleBoost: Option[Float],
    defaultMinShouldMatch: Option[String],
    scriptScoreFunctions: Set[ScriptScoreFunction])
  extends BaseDocumentClient {

  private def mergeDefaultScoringParams(scoringParams: ScoringParamSet) = {
    val requestedBoosts = scoringParams.fieldBoosts
    val requestedMsm = scoringParams.minShouldMatch
    val allBoosts = defaultTitleBoost
      .map(boost => Map(TitleFieldType -> boost) ++ requestedBoosts)
      .getOrElse(requestedBoosts)
    val finalMsm = if (requestedMsm.nonEmpty) requestedMsm else defaultMinShouldMatch
    scoringParams.copy(fieldBoosts = allBoosts, minShouldMatch = finalMsm)
  }

  // Assumes validation has already been done
  //
  // Called by buildSearchRequest and buildCountRequest
  //
  // * Chooses query type to be used and constructs query with applicable boosts
  // * Applies function scores (typically views and score) with applicable domain boosts
  // * Applies filters (facets and searchContext-sensitive federation preferences)
  private def buildBaseRequest(
      domainSet: DomainSet,
      searchParams: SearchParamSet,
      scoringParams: ScoringParamSet,
      user: Option[User],
      requireAuth: Boolean)
    : SearchRequestBuilder = {

    // Construct basic match query
    val matchQuery = chooseMatchQuery(searchParams.searchQuery, mergeDefaultScoringParams(scoringParams), user)

    // Wrap basic match query in filtered query for filtering
    val filteredQuery = compositeFilteredQuery(domainSet, searchParams, matchQuery, user, requireAuth)

    // Wrap filtered query in function score query for boosting
    val query = QueryBuilders.functionScoreQuery(filteredQuery)
    Boosts.applyScoreFunctions(query, scriptScoreFunctions)
    Boosts.applyDatatypeBoosts(query, scoringParams.datatypeBoosts)
    Boosts.applyDomainBoosts(query, domainSet.domainIdBoosts)
    query.scoreMode("multiply").boostMode("replace")

    val preparedSearch = esClient.client
      .prepareSearch(indexAliasName)
      .setQuery(query)
      .setTypes(esDocumentType)

    preparedSearch
  }

  def buildAutocompleteSearchRequest(
      domainSet: DomainSet,
      searchParams: SearchParamSet,
      scoringParams: ScoringParamSet,
      pagingParams: PagingParamSet,
      user: Option[User],
      requireAuth: Boolean): SearchRequestBuilder = {
    // Construct basic match query against title autocomplete field
    val query = searchParams.searchQuery match {
      case SimpleQuery(queryString) => autocompleteQuery(queryString)
      case _ => throw new MissingRequiredParameterError("q", "search query")
    }

    val filteredQuery = compositeFilteredQuery(domainSet, searchParams, query, user, requireAuth)

    val fnScoreQuery = QueryBuilders.functionScoreQuery(filteredQuery)
    Boosts.applyScoreFunctions(fnScoreQuery, scriptScoreFunctions)
    Boosts.applyDatatypeBoosts(fnScoreQuery, scoringParams.datatypeBoosts)
    Boosts.applyDomainBoosts(fnScoreQuery, domainSet.domainIdBoosts)
    fnScoreQuery.scoreMode("multiply").boostMode("replace")

    esClient.client
      .prepareSearch(indexAliasName)
      .setFrom(pagingParams.offset)
      .setSize(pagingParams.limit)
      .setQuery(fnScoreQuery)
      .setTypes(esDocumentType)
      .addField(TitleFieldType.fieldName)
      .addHighlightedField(TitleFieldType.autocompleteFieldName)
      .setHighlighterType("fvh")
      .setHighlighterPreTags("<span class=highlight>")
      .setHighlighterPostTags("</span>")
  }

  def buildSearchRequest(
      domainSet: DomainSet,
      searchParams: SearchParamSet,
      scoringParams: ScoringParamSet,
      pagingParams: PagingParamSet,
      user: Option[User],
      requireAuth: Boolean)
    : SearchRequestBuilder = {

    val baseRequest = buildBaseRequest(domainSet, searchParams, scoringParams, user, requireAuth)

    // WARN: Sort will totally blow away score if score isn't part of the sort
    // "Relevance" without a query can mean different things, so chooseSort decides
    val sort = pagingParams.sortOrder match {
      case Some(so) if so != "relevance" => Sorts.paramSortMap.get(so).get // will raise if invalid param got through
      case _ => Sorts.chooseSort(domainSet.searchContext, searchParams)
    }

    baseRequest
      .setFrom(pagingParams.offset)
      .setSize(pagingParams.limit)
      .addSort(sort)
  }

  def buildCountRequest(
      field: DocumentFieldType with Countable with Rawable,
      domainSet: DomainSet,
      searchParams: SearchParamSet,
      user: Option[User],
      requireAuth: Boolean)
    : SearchRequestBuilder = {

    val aggregation = chooseAggregation(field)

    val baseRequest = buildBaseRequest(domainSet, searchParams, ScoringParamSet(), user, requireAuth)

    baseRequest
      .addAggregation(aggregation)
      .setSearchType("count")
      .setSize(0) // no docs, aggs only
  }

  def buildFacetRequest(
      domainSet: DomainSet,
      searchParams: SearchParamSet,
      user: Option[User],
      requireAuth: Boolean)
  : SearchRequestBuilder = {
    val aggSize = 0 // agg count unlimited

    val datatypeAgg = AggregationBuilders
      .terms("datatypes")
      .field(DatatypeFieldType.fieldName)
      .size(aggSize)

    val categoryAgg = AggregationBuilders
      .terms("categories")
      .field(DomainCategoryFieldType.rawFieldName)
      .size(aggSize)

    val tagAgg = AggregationBuilders
      .terms("tags")
      .field(DomainTagsFieldType.rawFieldName)
      .size(aggSize)

    val metadataAgg = AggregationBuilders
      .nested("metadata")
      .path(DomainMetadataFieldType.fieldName)
      .subAggregation(AggregationBuilders.terms("keys")
        .field(DomainMetadataFieldType.Key.rawFieldName)
        .size(aggSize)
        .subAggregation(AggregationBuilders.terms("values")
          .field(DomainMetadataFieldType.Value.rawFieldName)
          .size(aggSize)))

    val baseRequest = buildBaseRequest(domainSet, searchParams, ScoringParamSet(), user, requireAuth)

    baseRequest
      .addAggregation(datatypeAgg)
      .addAggregation(categoryAgg)
      .addAggregation(tagAgg)
      .addAggregation(metadataAgg)
      .setSearchType("count")
      .setSize(0) // no docs, aggs only
  }
}
