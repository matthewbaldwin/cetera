package com.socrata.cetera.search

import org.elasticsearch.index.query.MatchQueryBuilder.Type.PHRASE
import org.elasticsearch.index.query.QueryBuilders.{boolQuery, matchQuery, nestedQuery}
import org.elasticsearch.index.query._

import com.socrata.cetera.auth.User
import com.socrata.cetera.esDomainType
import com.socrata.cetera.handlers.{ScoringParamSet, SearchParamSet}
import com.socrata.cetera.search.MultiMatchers.buildQuery
import com.socrata.cetera.types._

object DocumentQueries {

  private def applyClassificationQuery(
      query: BaseQueryBuilder,
      searchParams: SearchParamSet,
      withinSearchContext: Boolean)
    : BaseQueryBuilder = {

    // If there is no search context, use the ODN categories and tags
    // otherwise use the custom domain categories and tags
    val categoriesAndTags: Seq[QueryBuilder] =
      if (withinSearchContext) {
        List.concat(
          domainCategoriesQuery(searchParams.categories),
          domainTagsQuery(searchParams.tags))
      } else {
        List.concat(
          categoriesQuery(searchParams.categories),
          tagsQuery(searchParams.tags))
      }

    if (categoriesAndTags.nonEmpty) {
      categoriesAndTags.foldLeft(QueryBuilders.boolQuery().must(query)) { (b, q) => b.must(q) }
    } else {
      query
    }
  }

  def chooseMatchQuery(
      searchQuery: QueryType,
      scoringParams: ScoringParamSet,
      user: Option[User])
  : BaseQueryBuilder =
    searchQuery match {
      case NoQuery => noQuery
      case AdvancedQuery(queryString) => advancedQuery(queryString, scoringParams.fieldBoosts)
      case SimpleQuery(queryString) => simpleQuery(queryString, scoringParams, user)
    }

  def noQuery: BoolQueryBuilder =
    QueryBuilders.boolQuery().must(QueryBuilders.matchAllQuery())

  // This query is complex, as it generates two queries that are then combined
  // into a single query. By default, the must match clause enforces a term match
  // such that one or more of the query terms must be present in at least one of the
  // fields specified. The optional minimum_should_match constraint applies to this
  // clause. The should clause is intended to give a subset of retrieved results boosts
  // based on:
  //
  //   1. better phrase matching in the case of multiterm queries
  //   2. matches in particular fields (specified in fieldBoosts)
  //
  // The scores are then averaged together by ES with a defacto score of 0 for a should
  // clause if it does not in fact match any documents. See the ElasticSearch
  // documentation here:
  //
  //   https://www.elastic.co/guide/en/elasticsearch/guide/current/proximity-relevance.html
  def simpleQuery(
      queryString: String,
      scoringParams: ScoringParamSet,
      user: Option[User])
    : BoolQueryBuilder = {
    val matchTerms = buildQuery(queryString, MultiMatchQueryBuilder.Type.CROSS_FIELDS, scoringParams, user)
    val matchPhrase = buildQuery(queryString, MultiMatchQueryBuilder.Type.PHRASE, scoringParams, user)
    // terms must match and phrases should match
    QueryBuilders.boolQuery().must(matchTerms).should(matchPhrase)
  }

  // The advanced query allows a user to directly pass in lucene queries
  // NOTE: Advanced queries respect fieldBoosts but not datatypeBoosts
  // Q: Is this expected and desired?
  def advancedQuery(
      queryString: String,
      fieldBoosts: Map[CeteraFieldType with Boostable, Float])
    : BoolQueryBuilder = {

    val documentQuery = QueryBuilders
      .queryStringQuery(queryString)
      .field(FullTextSearchAnalyzedFieldType.fieldName)
      .field(FullTextSearchRawFieldType.fieldName)
      .autoGeneratePhraseQueries(true)

    val domainQuery = QueryBuilders
      .queryStringQuery(queryString)
      .field(FullTextSearchAnalyzedFieldType.fieldName)
      .field(FullTextSearchRawFieldType.fieldName)
      .field(DomainCnameFieldType.fieldName)
      .autoGeneratePhraseQueries(true)

    fieldBoosts.foreach { case (field, weight) =>
      documentQuery.field(field.fieldName, weight)
      domainQuery.field(field.fieldName, weight)
    }

    QueryBuilders.boolQuery()
      .should(documentQuery)
      .should(QueryBuilders.hasParentQuery(esDomainType, domainQuery))
  }

  def autocompleteQuery(queryString: String): MatchQueryBuilder =
    QueryBuilders.matchQuery(TitleFieldType.autocompleteFieldName, queryString)

  def compositeFilteredQuery(
      domainSet: DomainSet,
      searchParams: SearchParamSet,
      query: BaseQueryBuilder,
      user: Option[User],
      requireAuth: Boolean)
    : BaseQueryBuilder = {

    val categoriesAndTagsQuery = applyClassificationQuery(query, searchParams, domainSet.searchContext.isDefined)

    // This is a FilterBuilder, which incorporates all of the remaining constraints.
    // These constraints determine whether a document is considered part of the selection set, but
    // they do not affect the relevance score of the document.
    val compositeFilter = DocumentFilters().compositeFilter(domainSet, searchParams, user, requireAuth)
    QueryBuilders.filteredQuery(categoriesAndTagsQuery, compositeFilter)
  }

  def categoriesQuery(categories: Option[Set[String]]): Option[QueryBuilder] =
    categories.map { cs =>
      nestedQuery(
        CategoriesFieldType.fieldName,
        cs.foldLeft(boolQuery().minimumNumberShouldMatch(1)) { (b, q) =>
          b.should(matchQuery(CategoriesFieldType.Name.fieldName, q).`type`(PHRASE))
        }
      )
    }

  def tagsQuery(tags: Option[Set[String]]): Option[QueryBuilder] =
    tags.map { tags =>
      nestedQuery(
        TagsFieldType.fieldName,
        tags.foldLeft(boolQuery().minimumNumberShouldMatch(1)) { (b, q) =>
          b.should(matchQuery(TagsFieldType.Name.fieldName, q).`type`(PHRASE))
        }
      )
    }

  def domainCategoriesQuery(categories: Option[Set[String]]): Option[QueryBuilder] =
    categories.map { cs =>
      cs.foldLeft(boolQuery().minimumNumberShouldMatch(1)) { (b, q) =>
        b.should(matchQuery(DomainCategoryFieldType.fieldName, q).`type`(PHRASE))
      }
    }

  def domainTagsQuery(tags: Option[Set[String]]): Option[QueryBuilder] =
    tags.map { ts =>
      ts.foldLeft(boolQuery().minimumNumberShouldMatch(1)) { (b, q) =>
        b.should(matchQuery(DomainTagsFieldType.fieldName, q).`type`(PHRASE))
      }
    }
}
