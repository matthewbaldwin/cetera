package com.socrata.cetera.search

import org.elasticsearch.index.query._
import org.elasticsearch.index.query.QueryBuilders._

import com.socrata.cetera.types.{DomainCnameFieldType, IsCustomerDomainFieldType}
import com.socrata.cetera.esDomainType

object DomainQueries {
  def idQuery(domainIds: Set[Int]): IdsQueryBuilder =
    idsQuery().types(esDomainType).addIds(domainIds.map(_.toString).toSeq: _*)

  def cnamesQuery(domainCnames: Set[String]): TermsQueryBuilder =
    termsQuery(DomainCnameFieldType.rawFieldName, domainCnames.toSeq: _*)

  // two nos make a yes: this filters out items with is_customer_domain=false, while permitting true or null.
  def isNotCustomerDomainQuery: TermQueryBuilder =
    termQuery(IsCustomerDomainFieldType.fieldName, false)

  def isCustomerDomainQuery: BoolQueryBuilder =
    boolQuery().mustNot(isNotCustomerDomainQuery)
}
