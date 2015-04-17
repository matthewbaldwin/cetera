package com.socrata.cetera.search

import java.io.Closeable

import org.elasticsearch.action.search.{SearchRequestBuilder, SearchResponse}
import org.elasticsearch.client.Client
import org.elasticsearch.client.transport.TransportClient
import org.elasticsearch.common.settings.ImmutableSettings
import org.elasticsearch.common.transport.InetSocketTransportAddress
import org.elasticsearch.index.query.{FilterBuilders, QueryBuilders}
import org.elasticsearch.search.aggregations.AggregationBuilders
import org.elasticsearch.search.aggregations.bucket.terms.Terms
import org.elasticsearch.search.sort.{SortBuilders, SortOrder}

import com.socrata.cetera.types.CeteraFieldType
import com.socrata.cetera.types._

// Elastic Search Field Translator
//
// There is some confusion here because semantic types and implementation details are out of sync.
// categories and tags are nested in ES whereas domain_cname is not.

class ElasticSearchClient(host: String, port: Int, clusterName: String) extends Closeable {
  val settings = ImmutableSettings.settingsBuilder()
                   .put("cluster.name", clusterName)
                   .put("client.transport.sniff", true)
                   .build()

  val client: Client = new TransportClient(settings)
    .addTransportAddress(new InetSocketTransportAddress(host, port))

  def close(): Unit = client.close()

  implicit class FieldTypeToFieldName(field: CeteraFieldType) {
    def fieldName: String = {
      field match {
        case DomainFieldType => "socrata_id.domain_cname"

        case CategoriesFieldType => "animl_annotations.categories"
        case TagsFieldType => "animl_annotations.tags"

        case TitleFieldType => "indexed_metadata.name"
        case DescriptionFieldType => "indexed_metadata.description"
      }
    }
  }


  implicit class FieldTypeToRawFieldName(field: CeteraFieldType with Countable) {
    def rawFieldName: String = {
      field match {
        case DomainFieldType => "socrata_id.domain_cname.raw"
        case CategoriesFieldType => "animl_annotations.categories.name.raw"
        case TagsFieldType => "animl_annotations.tags.name.raw"
      }
    }
  }
  // NOTE: domain_cname does not currently have a score associated with it
  implicit class FieldTypeToScoreFieldName(field: CeteraFieldType with Countable with Scorable) {
    def scoreFieldName: String = {
      field match {
        case CategoriesFieldType => "animl_annotations.categories.score"
        case TagsFieldType => "animl_annotations.tags.score"
      }
    }
  }

  // Assumes validation has already been done
  def buildBaseRequest(searchQuery: Option[String],
                       domains: Option[Set[String]],
                       categories: Option[Set[String]],
                       tags: Option[Set[String]],
                       only: Option[String],
                       boosts: Map[CeteraFieldType with Boostable, Float]): SearchRequestBuilder = {

    val matchQuery = searchQuery match {
      case None =>
        QueryBuilders.matchAllQuery

      case Some(sq) if boosts.isEmpty =>
        QueryBuilders.matchQuery("_all", sq)

      case Some(sq) =>
        val text_args = boosts.map {
          case (field, weight) =>
            val fieldName = field.fieldName
            s"${fieldName}^${weight}" // NOTE ^ does not mean exponentiate, it means multiply
        } ++ List("_all")

        QueryBuilders.multiMatchQuery(sq, text_args.toList:_*)
    }

    val query = locally {
      val domainFilter = domains.map { domains =>
        FilterBuilders.termsFilter(DomainFieldType.rawFieldName, domains.toSeq:_*)
      }

      val categoriesFilter = categories.map { categories =>
        FilterBuilders.nestedFilter(CategoriesFieldType.fieldName,
          FilterBuilders.termsFilter(CategoriesFieldType.rawFieldName, categories.toSeq:_*))
      }

      val tagsFilter = tags.map { tags =>
        FilterBuilders.nestedFilter(TagsFieldType.fieldName,
          FilterBuilders.termsFilter(TagsFieldType.rawFieldName, tags.toSeq:_*))
      }

      val filters = List(domainFilter, categoriesFilter, tagsFilter).flatten

      if (filters.nonEmpty) {
        QueryBuilders.filteredQuery(matchQuery, FilterBuilders.andFilter(filters:_*))
      } else {
        matchQuery
      }
    }

    // Imperative builder --> order is important
    client
      .prepareSearch("datasets", "pages") // literals should not be here
      .setTypes(only.toList:_*)
      .setQuery(query)
  }

  def buildSearchRequest(searchQuery: Option[String],
                         domains: Option[Set[String]],
                         categories: Option[Set[String]],
                         tags: Option[Set[String]],
                         only: Option[String],
                         boosts: Map[CeteraFieldType with Boostable, Float],
                         offset: Int,
                         limit: Int): SearchRequestBuilder = {

    val baseRequest = buildBaseRequest(
      searchQuery,
      domains,
      categories,
      tags,
      only,
      boosts
    )

    // First pass logic is very simple. query >> categories >> tags
    val sort = (searchQuery, categories, tags) match {
      case (None, None, None) =>
        SortBuilders
          .scoreSort()
          .order(SortOrder.DESC)

      // Query
      case (Some(sq), _, _) =>
        SortBuilders
          .scoreSort()
          .order(SortOrder.DESC)

      // Categories
      case (_, Some(cats), _) =>
        SortBuilders
          .fieldSort("animl_annotations.categories.score")
          .order(SortOrder.DESC)
          .sortMode("max")
          .setNestedFilter(FilterBuilders.termsFilter(CategoriesFieldType.rawFieldName, cats.toSeq:_*))

      // Tags
      case (_, _, Some(ts)) =>
        SortBuilders
          .fieldSort("animl_annotations.tags.score")
          .order(SortOrder.DESC)
          .sortMode("max")
          .setNestedFilter(FilterBuilders.termsFilter(TagsFieldType.rawFieldName, ts.toSeq:_*))
    }

    baseRequest
      .setFrom(offset)
      .setSize(limit)
      .addSort(sort)
  }

  def buildCountRequest(field: CeteraFieldType with Countable,
                        searchQuery: Option[String],
                        domains: Option[Set[String]],
                        categories: Option[Set[String]],
                        tags: Option[Set[String]],
                        only: Option[String]): SearchRequestBuilder = {

    val baseRequest = buildBaseRequest(
      searchQuery,
      domains,
      categories,
      tags,
      only,
      Map.empty
    )

    val aggregation = field match {
      case DomainFieldType =>
        AggregationBuilders
          .terms("domains")
          .field(field.rawFieldName)
          .order(Terms.Order.count(false)) // count desc
          .size(0) // unlimited

      case CategoriesFieldType =>
        AggregationBuilders
          .nested("annotations")
          .path(field.fieldName)
          .subAggregation(
            AggregationBuilders
              .terms("names")
              .field(field.rawFieldName)
          )

      case TagsFieldType =>
        AggregationBuilders
          .nested("annotations")
          .path(field.fieldName)
          .subAggregation(
            AggregationBuilders
              .terms("names")
              .field(field.rawFieldName)
          )
    }

    baseRequest
      .addAggregation(aggregation)
      .setSearchType("count")
  }
}
