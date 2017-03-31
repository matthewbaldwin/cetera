package com.socrata.cetera.search

import scala.language.existentials

import org.elasticsearch.action.search.SearchRequestBuilder
import org.elasticsearch.search.aggregations.AggregationBuilders
import org.elasticsearch.search.fetch.subphase.highlight.HighlightBuilder
import org.elasticsearch.search.fetch.subphase.highlight.SearchContextHighlight.FieldOptions

import com.socrata.cetera.auth.User
import com.socrata.cetera.errors.MissingRequiredParameterError
import com.socrata.cetera.esDocumentType
import com.socrata.cetera.handlers.{PagingParamSet, ScoringParamSet, SearchParamSet}
import com.socrata.cetera.search.DocumentAggregations.chooseAggregation
import com.socrata.cetera.types._

trait BaseDocumentClient {
  def buildSearchRequest(
      domainSet: DomainSet,
      searchParams: SearchParamSet,
      scoringParams: ScoringParamSet,
      pagingParams: PagingParamSet,
      user: Option[User])
    : SearchRequestBuilder

  def buildAutocompleteSearchRequest(
      domainSet: DomainSet,
      searchParams: SearchParamSet,
      scoringParams: ScoringParamSet,
      pagingParams: PagingParamSet,
      user: Option[User])
    : SearchRequestBuilder

  def buildCountRequest(
      field: DocumentFieldType with Countable with Rawable,
      domainSet: DomainSet,
      searchParams: SearchParamSet,
      pagingParams: PagingParamSet,
      user: Option[User])
    : SearchRequestBuilder

  def buildFacetRequest(
      domainSet: DomainSet,
      searchParams: SearchParamSet,
      pagingParams: PagingParamSet,
      user: Option[User])
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
      user: Option[User])
    : SearchRequestBuilder = {

    // Construct basic match query
    val matchQuery = DocumentQuery().chooseMatchQuery(
      searchParams.searchQuery, mergeDefaultScoringParams(scoringParams), user)

    // Wrap basic match query in filtered query for filtering
    val filteredQuery = DocumentQuery().compositeFilteredQuery(
      domainSet, searchParams, matchQuery, user)

    // Wrap filtered query in function score query for boosting
    val query = DocumentQuery().scoringQuery(
      filteredQuery, domainSet, scriptScoreFunctions, scoringParams)

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
      user: Option[User])
    : SearchRequestBuilder = {
    // Construct basic match query against title autocomplete field
    val matchQuery = searchParams.searchQuery match {
      case SimpleQuery(queryString) => DocumentQuery().autocompleteQuery(queryString)
      case _ => throw new MissingRequiredParameterError("q", "search query")
    }

    val filteredQuery = DocumentQuery().compositeFilteredQuery(
      domainSet, searchParams, matchQuery, user)

    // Wrap filtered query in function score query for boosting
    val query = DocumentQuery().scoringQuery(
      filteredQuery, domainSet, scriptScoreFunctions, scoringParams)

    val highlighterOptions = new FieldOptions()
      .preTags()

    val highlighter = new HighlightBuilder()
      .field(TitleFieldType.autocompleteFieldName)
      .highlighterType("fvh")
      .preTags("<span class=highlight>")
      .postTags("</span>")

    esClient.client
      .prepareSearch(indexAliasName)
      .setFrom(pagingParams.offset)
      .setSize(pagingParams.limit)
      .setQuery(query)
      .setTypes(esDocumentType)
      .setFetchSource(Array(TitleFieldType.fieldName), Array.empty[String])
      .highlighter(highlighter)
  }

  def buildSearchRequest(
      domainSet: DomainSet,
      searchParams: SearchParamSet,
      scoringParams: ScoringParamSet,
      pagingParams: PagingParamSet,
      user: Option[User])
    : SearchRequestBuilder = {

    val baseRequest = buildBaseRequest(domainSet, searchParams, scoringParams, user)

    // WARN: Sort will totally blow away score if score isn't part of the sort
    // "Relevance" without a query can mean different things, so chooseSort decides
    val sort = pagingParams.sortOrder match {
      case Some(so) if so != "relevance" =>
        Sorts.mapSortParam(so).get // will raise if invalid param got through
      case _ =>
        Sorts.chooseSort(domainSet.searchContext, searchParams)
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
      pagingParams: PagingParamSet,
      user: Option[User])
    : SearchRequestBuilder = {

    val aggregation = chooseAggregation(field, pagingParams.limit)

    val baseRequest = buildBaseRequest(domainSet, searchParams, ScoringParamSet(), user)

    baseRequest
      .addAggregation(aggregation)
      .setSize(0) // no docs, aggs only
  }

  def buildFacetRequest(
      domainSet: DomainSet,
      searchParams: SearchParamSet,
      pagingParams: PagingParamSet,
      user: Option[User])
    : SearchRequestBuilder = {
    val datatypeAgg = AggregationBuilders
      .terms("datatypes")
      .field(DatatypeFieldType.fieldName)
      .size(pagingParams.limit)

    val categoryAgg = AggregationBuilders
      .terms("categories")
      .field(DomainCategoryFieldType.rawFieldName)
      .size(pagingParams.limit)

    val tagAgg = AggregationBuilders
      .terms("tags")
      .field(DomainTagsFieldType.rawFieldName)
      .size(pagingParams.limit)

    val metadataAgg = AggregationBuilders
      .nested("metadata", DomainMetadataFieldType.fieldName)
      .subAggregation(AggregationBuilders.terms("keys")
        .field(DomainMetadataFieldType.Key.rawFieldName)
        .size(pagingParams.limit)
        .subAggregation(AggregationBuilders.terms("values")
          .field(DomainMetadataFieldType.Value.rawFieldName)
          .size(pagingParams.limit)))

    val baseRequest = buildBaseRequest(domainSet, searchParams, ScoringParamSet(), user)

    baseRequest
      .addAggregation(datatypeAgg)
      .addAggregation(categoryAgg)
      .addAggregation(tagAgg)
      .addAggregation(metadataAgg)
      .setSize(0) // no docs, aggs only
  }
}
