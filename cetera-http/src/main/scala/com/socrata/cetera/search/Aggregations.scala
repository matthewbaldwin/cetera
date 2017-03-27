package com.socrata.cetera.search

import org.elasticsearch.index.query.QueryBuilder
import org.elasticsearch.search.aggregations.bucket.nested.NestedAggregationBuilder
import org.elasticsearch.search.aggregations.bucket.terms.{Terms, TermsAggregationBuilder}
import org.elasticsearch.search.aggregations.{AggregationBuilder, AggregationBuilders}

import com.socrata.cetera.esDocumentType
import com.socrata.cetera.types._

object DocumentAggregations {
  // The 'terms' used in the AggregationBuilders below need to be accounted for in the CountService val 'pattern'.

  def domainCategories(aggSize: Int): TermsAggregationBuilder =
    AggregationBuilders
      .terms("domain_categories")
      .field(DomainCategoryFieldType.rawFieldName)
      .order(Terms.Order.count(false)) // count desc
      .size(aggSize)

  def domainTags(aggSize: Int): TermsAggregationBuilder =
    AggregationBuilders
      .terms("domain_tags")
      .field(DomainTagsFieldType.rawFieldName)
      .order(Terms.Order.count(false)) // count desc
      .size(aggSize)

  def categories(aggSize: Int): NestedAggregationBuilder =
    AggregationBuilders
      .nested("annotations", CategoriesFieldType.fieldName)
      .subAggregation(
        AggregationBuilders
          .terms("names")
          .field(CategoriesFieldType.Name.rawFieldName)
          .size(aggSize)
      )

  def tags(aggSize: Int): NestedAggregationBuilder =
    AggregationBuilders
      .nested("annotations", TagsFieldType.fieldName)
      .subAggregation(
        AggregationBuilders
          .terms("names")
          .field(TagsFieldType.Name.rawFieldName)
          .size(aggSize)
      )

  def owners(aggSize: Int): TermsAggregationBuilder =
    AggregationBuilders
      .terms("owners")
      .field(OwnerIdFieldType.rawFieldName)
      .order(Terms.Order.count(false)) // count desc
      .size(aggSize)

  def attributions(aggSize: Int): TermsAggregationBuilder =
    AggregationBuilders
      .terms("attributions")
      .field(AttributionFieldType.rawFieldName)
      .order(Terms.Order.count(false)) // count desc
      .size(aggSize)

  def provenance(aggSize: Int): TermsAggregationBuilder =
    AggregationBuilders
      .terms("provenance")
      .field(ProvenanceFieldType.rawFieldName)
      .order(Terms.Order.count(false))
      .size(aggSize)

  def license(aggSize: Int): TermsAggregationBuilder =
    AggregationBuilders
      .terms("license")
      .field(LicenseFieldType.rawFieldName)
      .order(Terms.Order.count(false))
      .size(aggSize)

  def chooseAggregation(
      field: DocumentFieldType with Countable with Rawable,
      aggSize: Int)
    : AggregationBuilder =
    field match {
      case DomainCategoryFieldType => domainCategories(aggSize)
      case DomainTagsFieldType => domainTags(aggSize)

      case CategoriesFieldType => categories(aggSize)
      case TagsFieldType => tags(aggSize)

      // TODO: remove these unused aggregations
      case OwnerIdFieldType => owners(aggSize)
      case AttributionFieldType => attributions(aggSize)
      case ProvenanceFieldType => provenance(aggSize)
      case LicenseFieldType => license(aggSize)
    }
}

object DomainAggregations {
  def domains(
      docQuery: QueryBuilder,
      aggSize: Int)
    : TermsAggregationBuilder =
    AggregationBuilders
      .terms("domains") // "domains" is an agg of terms on field "domain_cname.raw"
      .field("domain_cname.raw")
      .size(aggSize)
      .subAggregation(
        AggregationBuilders
          .children("documents", esDocumentType) // "documents" is an agg of children of type esDocumentType
          .subAggregation(
            AggregationBuilders
              .filter("filtered", docQuery) // "filtered" is an aggregation of documents that match this filter
          )
    )
}
