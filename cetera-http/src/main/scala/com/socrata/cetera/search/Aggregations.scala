package com.socrata.cetera.search

import org.elasticsearch.index.query.FilterBuilders
import org.elasticsearch.search.aggregations.bucket.terms.Terms
import org.elasticsearch.search.aggregations.{AbstractAggregationBuilder, AggregationBuilders}
import org.elasticsearch.search.aggregations.bucket.terms.Terms

import com.socrata.cetera._
import com.socrata.cetera.types._

object DocumentAggregations {
  // The 'terms' and 'nested' fields need to jive with
  // ../services/CountService.scala

  private val aggSize = 0 // agg count unlimited

  val domainCategories =
    AggregationBuilders
      .terms("domain_categories")
      .field(DomainCategoryFieldType.rawFieldName)
      .order(Terms.Order.count(false)) // count desc
      .size(aggSize)

  val domainTags =
    AggregationBuilders
      .terms("domain_tags")
      .field(DomainTagsFieldType.rawFieldName)
      .order(Terms.Order.count(false)) // count desc
      .size(aggSize)

  val categories =
    AggregationBuilders
      .nested("annotations")
      .path(CategoriesFieldType.fieldName)
      .subAggregation(
        AggregationBuilders
          .terms("names")
          .field(CategoriesFieldType.Name.rawFieldName)
          .size(aggSize)
      )

  val tags =
    AggregationBuilders
      .nested("annotations")
      .path(TagsFieldType.fieldName)
      .subAggregation(
        AggregationBuilders
          .terms("names")
          .field(TagsFieldType.Name.rawFieldName)
          .size(aggSize)
      )

  def chooseAggregation(field: DocumentFieldType with Countable with Rawable): AbstractAggregationBuilder =
    field match {
      case DomainCategoryFieldType => domainCategories
      case DomainTagsFieldType => domainTags

      case CategoriesFieldType => categories
      case TagsFieldType => tags
    }
}

object DomainAggregations {
  private val aggSize = 0 // agg count unlimited

  def domains(searchContextIsModerated: Boolean,
              moderatedDomainIds: Set[Int],
              unmoderatedDomainIds: Set[Int],
              routingApprovalDisabledDomainIds: Set[Int]
             ): AbstractAggregationBuilder = {
    val publicFilter = DocumentFilters.publicFilter(isDomainAgg = true)
    val moderationFilter = DocumentFilters.moderationStatusFilter(
      searchContextIsModerated,
      moderatedDomainIds,
      unmoderatedDomainIds,
      isDomainAgg = true
    )
    val routingApprovalFilter = DocumentFilters.routingApprovalFilter(
      None,
      routingApprovalDisabledDomainIds,
      isDomainAgg = true
    )
    AggregationBuilders
      .terms("domains") // "domains" is an agg of terms on field "domain_cname.raw"
      .field("domain_cname.raw")
      .size(aggSize)
      .subAggregation(
        AggregationBuilders
          .children("documents") // "documents" is an agg of children of type esDocumentType
          .childType(esDocumentType)
          .subAggregation(
            AggregationBuilders
              .filter("visible") // "visible" is an agg of documents matching the following filter
              .filter(FilterBuilders.boolFilter()
                // is customer domain filter must be applied above this aggregation
                // apply isPublic filter
                .must(publicFilter)
                // apply moderation filter
                .must(moderationFilter)
                // apply routing & approval filter
                .must(routingApprovalFilter)
              )
          )
      )
  }
}