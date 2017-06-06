package com.socrata.cetera.search

import com.rojoma.json.v3.codec.JsonEncode
import com.rojoma.json.v3.interpolation._
import com.rojoma.json.v3.io.JsonReader
import org.elasticsearch.index.query.{TermQueryBuilder, TermsQueryBuilder}
import org.scalatest.{ShouldMatchers, WordSpec}

import com.socrata.cetera.TestESDomains
import com.socrata.cetera.auth.AuthedUser
import com.socrata.cetera.errors.UnauthorizedError
import com.socrata.cetera.handlers.{ScoringParamSet, SearchParamSet}
import com.socrata.cetera.types._

class DocumentQueriesSpec extends WordSpec with ShouldMatchers with TestESDomains {

  val context = Some("example.com")
  val queryString = "snuffy"
  val emptyFieldBoosts = Map.empty[CeteraFieldType with Boostable, Float]
  val fieldBoosts = Map[CeteraFieldType with Boostable, Float](TitleFieldType -> 2.2f)
  val emptyDatatypeBoosts = Map.empty[Datatype, Float]
  val datatypeBoosts = Map[Datatype, Float](Datatype("datasets").get -> 1.0f)
  val queryDefaults = """
      "disable_coord": false,
      "adjust_pure_negative": true,
      "boost": 1.0
  """

  val docQuery = DocumentQuery(forDomainSearch = false)
  val docQueryForDomainSearch = DocumentQuery(forDomainSearch = true)

  def behaveLikeABooleanTermQuery(
      query: Boolean => TermQueryBuilder,
      termKey: String,
      forDomainSearch: Boolean)
    : Unit = {
    s"return the expected boolean term query with key $termKey" in {
      val trueParamQuery = query(true)
      val falseParamQuery = query(false)
      val expectedTrue = j"""{"term": {$termKey: {"value": true, "boost": 1.0}}}"""
      val expectedFalse = j"""{"term": {$termKey: {"value": false, "boost": 1.0}}}"""
      val actualTrue = JsonReader.fromString(trueParamQuery.toString)
      val actuaFalse = JsonReader.fromString(falseParamQuery.toString)
      actualTrue should be(expectedTrue)
      actuaFalse should be(expectedFalse)
    }
  }

  def behaveLikeATermQuery[T: JsonEncode](
      query: T => TermQueryBuilder,
      termKey: String,
      termValue: T,
      forDomainSearch: Boolean)
    : Unit = {
    s"return the expected term query with key $termKey and value $termValue" in {
      val expected = j"""{"term": {$termKey: {"value": $termValue, "boost": 1.0}}}"""
      val actual = JsonReader.fromString(query(termValue).toString)
      actual should be(expected)
    }
  }

  def behaveLikeAStatusTermQuery(
      query: ApprovalStatus => TermQueryBuilder,
      termKey: String,
      termValue: ApprovalStatus,
      forDomainSearch: Boolean)
    : Unit = {
    s"return the expected term query with key $key and value ${termValue.status}" in {
      val expected = j"""{"term": {$termKey: {"value": ${termValue.status}, "boost": 1.0}}}"""
      val actual = JsonReader.fromString(query(termValue).toString)
      actual should be(expected)
    }
  }

  def behaveLikeATermsQuery[T: JsonEncode](
      query: Set[T] => TermsQueryBuilder,
      termKey: String,
      termValues: Set[T],
      forDomainSearch: Boolean): Unit = {
    s"return the expected terms query with key $termKey and values $termValues" in {
      termValues.size should be > 1 // to test empty sets and sets of 1 and sets of many
      val singleTermValue = termValues.head
      val emptyQuery = query(Set.empty[T])
      val singleQuery = query(Set(singleTermValue))
      val multipleQuery = query(termValues)

      val actualEmpty = JsonReader.fromString(emptyQuery.toString)
      val actualSingle = JsonReader.fromString(singleQuery.toString)
      val actualMultiple = JsonReader.fromString(multipleQuery.toString)

      val expectedEmpty = j"""{"terms": {$termKey: [], "boost": 1.0}}"""
      val expectedSingle = j"""{"terms": {$termKey: [$singleTermValue], "boost": 1.0}}"""
      val expectedMultiple = j"""{"terms": {$termKey: $termValues, "boost": 1.0}}"""

      actualEmpty should be(expectedEmpty)
      actualSingle should be(expectedSingle)
      actualMultiple should be(expectedMultiple)
    }
  }

  "the datatypeQuery" should {
    behaveLikeATermsQuery(docQuery.datatypeQuery, "datatype", Set("dataset", "link"), forDomainSearch = false)
  }

  "the userQuery" should {
    behaveLikeATermQuery(docQuery.userQuery, "owner.id", "wonder-woman", forDomainSearch = false)
  }


  "the sharedToQuery" should {
    behaveLikeATermQuery(docQuery.sharedToQuery, "shared_to", "wonder-woman", forDomainSearch = false)
  }

  "the attributionQuery" should {
    behaveLikeATermQuery(docQuery.attributionQuery, "attribution.raw", "wonder-woman", forDomainSearch = false)
  }

  "the provenanceQuery" should {
    behaveLikeATermQuery(docQuery.provenanceQuery, "provenance", "official", forDomainSearch = false)
  }

  "the licenseQuery" should {
    behaveLikeATermQuery(docQuery.licenseQuery, "license", "WTFPL", forDomainSearch = false)
  }

  "the parentDatasetQuery" should {
    behaveLikeATermQuery(docQuery.parentDatasetQuery, "socrata_id.parent_dataset_id", "four-four", forDomainSearch = false)
  }

  "the hiddenFromCatalogQuery" should {
    behaveLikeABooleanTermQuery(docQuery.hiddenFromCatalogQuery, "hide_from_catalog", forDomainSearch = false)
  }

  "the idQuery" should {
    "return the expected ids query" in {
      val emptyQuery = docQuery.idQuery(Set.empty)
      val singleQuery = docQuery.idQuery(Set("abcd-1234"))
      val multipleQuery = docQuery.idQuery(Set("abcd-1234", "foob-arba"))

      val actualEmpty = JsonReader.fromString(emptyQuery.toString)
      val actualSingle = JsonReader.fromString(singleQuery.toString)
      val actualMultiple = JsonReader.fromString(multipleQuery.toString)

      val expectedEmpty = j"""{"ids": {"type": ["document"], "values": [], "boost": 1.0}}"""
      val expectedSingle = j"""{"ids": {"type": ["document"], "values": ["abcd-1234"], "boost": 1.0}}"""
      val expectedMultiple = j"""{"ids": {"type": ["document"], "values": ["abcd-1234", "foob-arba"], "boost": 1.0}}"""

      actualEmpty should be(expectedEmpty)
      actualSingle should be(expectedSingle)
      actualMultiple should be(expectedMultiple)
    }
  }

  "the domainIdQuery" should {
    behaveLikeATermsQuery(docQuery.domainIdQuery, "socrata_id.domain_id", Set(0, 1), forDomainSearch = false)
  }

  "the approved raStatusAccordingToParentDomainQuery" should {
    behaveLikeABooleanTermQuery(docQuery.raStatusAccordingToParentDomainQuery(ApprovalStatus.approved, _), "is_approved_by_parent_domain", forDomainSearch = false)
  }

  "the pending raStatusAccordingToParentDomainQuery" should {
    behaveLikeABooleanTermQuery(docQuery.raStatusAccordingToParentDomainQuery(ApprovalStatus.pending, _), "is_pending_on_parent_domain", forDomainSearch = false)
  }

  "the rejected raStatusAccordingToParentDomainQuery" should {
    behaveLikeABooleanTermQuery(docQuery.raStatusAccordingToParentDomainQuery(ApprovalStatus.rejected, _), "is_rejected_by_parent_domain", forDomainSearch = false)
  }

  "the approved raStatusAccordingToContextQuery" should {
    behaveLikeATermQuery[Int](docQuery.raStatusAccordingToContextQuery(ApprovalStatus.approved, _), "approving_domain_ids", 3, forDomainSearch = false)
  }

  "the pending raStatusAccordingToContextQuery" should {
    behaveLikeATermQuery[Int](docQuery.raStatusAccordingToContextQuery(ApprovalStatus.pending, _), "pending_domain_ids", 3, forDomainSearch = false)
  }

  "the rejected raStatusAccordingToContextQuery" should {
    behaveLikeATermQuery[Int](docQuery.raStatusAccordingToContextQuery(ApprovalStatus.rejected, _), "rejecting_domain_ids", 3, forDomainSearch = false)
  }

  "the approved modStatusQuery" should {
    behaveLikeAStatusTermQuery(docQuery.modStatusQuery, "moderation_status", ApprovalStatus.approved, forDomainSearch = false)
  }

  "the pending modStatusQuery" should {
    behaveLikeAStatusTermQuery(docQuery.modStatusQuery, "moderation_status", ApprovalStatus.pending, forDomainSearch = false)
  }

  "the rejected modStatusQuery" should {
    behaveLikeAStatusTermQuery(docQuery.modStatusQuery, "moderation_status", ApprovalStatus.rejected, forDomainSearch = false)
  }

  "the defaultViewQuery" should {
    behaveLikeABooleanTermQuery(docQuery.defaultViewQuery, "is_default_view", forDomainSearch = false)
  }

  "the publicQuery" should {
    behaveLikeABooleanTermQuery(docQuery.publicQuery, "is_public", forDomainSearch = false)
  }

  "the publishedQuery" should {
    behaveLikeABooleanTermQuery(docQuery.publishedQuery, "is_published", forDomainSearch = false)
  }

  "the customerCategoriesQuery" should {
    behaveLikeATermsQuery(docQuery.customerCategoriesQuery, "customer_category.raw", Set("cats", "dogs"), forDomainSearch = false)
  }

  "the customerTagsQuery" should {
    behaveLikeATermsQuery(docQuery.customerTagsQuery, "customer_tags.raw", Set("pumas", "cheetahs"), forDomainSearch = false)
  }

  "the privateMetadataUserRestrictionsFilter" should {
    "return the expected query when the user doesn't have a blessed role" in {
      val user = AuthedUser("user-fxf", domains(0))
      val query = docQuery.privateMetadataUserRestrictionsQuery(user)
      val expected = JsonReader.fromString(s"""
      {
          "bool": {
              "should": [
                  {"term": {"owner.id": {"value": "user-fxf", "boost": 1.0}}},
                  {"term": {"shared_to": {"value": "user-fxf", "boost": 1.0}}}
              ],
              $queryDefaults
          }
      }
      """)
      val actual = JsonReader.fromString(query.toString)
      actual should be(expected)
    }

    "return the expected filter when the user does have a blessed role" in {
      val user = AuthedUser("user-fxf", domains(0), Some("publisher"))
      val query = docQuery.privateMetadataUserRestrictionsQuery(user)
      val expected = JsonReader.fromString(s"""
      {
          "bool": {
              "should": [
                  {"term": {"owner.id": {"value": "user-fxf", "boost": 1.0}}},
                  {"term": {"shared_to": {"value": "user-fxf", "boost": 1.0}}},
                  {"terms": {"socrata_id.domain_id": [0], "boost": 1.0}}
              ],
              $queryDefaults
          }
      }
      """)
      val actual = JsonReader.fromString(query.toString)
      actual should be(expected)
    }
  }

  "the privateDomainMetadataQuery" should {
    "return None if no user is given" in {
      docQuery.privateMetadataQuery(Set.empty, None) should be(None)
    }

    "return a basic metadata query when the user is a super admin" in {
      val user = AuthedUser("mooks", domains(0), flags = Some(List("admin")))
      val metadata = Set(("org", "ny"))
      val actual = JsonReader.fromString(docQuery.privateMetadataQuery(metadata, Some(user)).get.toString)
      val expected = JsonReader.fromString(docQuery.metadataQuery(metadata, public = false).get.toString)
      actual should be(expected)
    }

    "return the expected filter when the user is authenticated" in {
      val user = AuthedUser("mooks", domains(0), Some("publisher"))
      val metadata = Set(("org", "ny"), ("org", "nj"))
      val query = docQuery.privateMetadataQuery(metadata, Some(user))
      val actual = JsonReader.fromString(query.get.toString)
      val metadataQuery = JsonReader.fromString(docQuery.metadataQuery(metadata, public = false).get.toString)
      val privacyQuery = JsonReader.fromString(docQuery.privateMetadataUserRestrictionsQuery(user).toString)
      val expected = JsonReader.fromString(s"""
      {
          "bool": {
              "must": [$privacyQuery, $metadataQuery],
              $queryDefaults
          }
      }
      """)
      actual should be(expected)
    }
  }

  "the combinedMetadataQuery" should {
    "return None if neither public or private metadata queries are created" in {
      val user = AuthedUser("mooks", domains(0), flags = Some(List("admin")))
      docQuery.combinedMetadataQuery(Set.empty, Some(user)) should be(None)
    }

    "return only a public metadata filter if the user isn't authenticated" in {
      val metadata = Set(("org", "ny"))
      val actual = JsonReader.fromString(docQuery.combinedMetadataQuery(metadata, None).get.toString)
      val expected = JsonReader.fromString(docQuery.metadataQuery(metadata, public = true).get.toString)
      actual should be(expected)
    }

    "return both public and private metadata queries if the user is authenticated" in {
      val user = AuthedUser("mooks", domains(0), Some("publisher"))
      val metadata = Set(("org", "ny"), ("org", "nj"))
      val query = docQuery.combinedMetadataQuery(metadata, Some(user))
      val actual = JsonReader.fromString(query.get.toString)
      val publicQuery = docQuery.metadataQuery(metadata, public = true).get.toString
      val privateQuery = docQuery.privateMetadataQuery(metadata, Some(user)).get.toString
      val expected = JsonReader.fromString(s"""
      {
          "bool": {
              "should": [$publicQuery, $privateQuery],
              $queryDefaults
          }
      }
      """)
      actual should be(expected)
    }
  }

  "the vmSearchContextQuery" should {
    "return None if there is no search context" in {
      val domainSet = DomainSet(Set(domains(0)), None)
      docQuery.vmSearchContextQuery(domainSet) should be(None)
    }

    "return None if the search context isn't moderated" in {
      val domainSet = DomainSet(Set(domains(0)), Some(domains(0)))
      docQuery.vmSearchContextQuery(domainSet) should be(None)
    }

    "return the expected query if the search context is moderated" in {
      val domainSet = DomainSet(Set(domains(1), domains(0)), Some(domains(1)))
      val query = docQuery.vmSearchContextQuery(domainSet).get
      val actual = JsonReader.fromString(query.toString)
      val defaultQuery = """{"term": {"is_default_view": {"value": true, "boost": 1.0}}}"""
      val fromModeratedDomain = """{"terms": {"socrata_id.domain_id": [1], "boost": 1.0}}"""
      val expected = JsonReader.fromString(s"""
      {
          "bool": {
              "should": [$defaultQuery, $fromModeratedDomain],
              $queryDefaults
          }
      }
      """)
      actual should be(expected)
    }
  }

  "the raSearchContextQuery" should {
    "return None if there is no search context" in {
      val domainSet = DomainSet(Set(domains(2)), None)
      docQuery.raSearchContextQuery(domainSet) should be(None)
    }

    "return None if the search context doesn't have R&A enabled" in {
      val domainSet = DomainSet(Set(domains(1)), Some(domains(1)))
      docQuery.raSearchContextQuery(domainSet) should be(None)
    }

    "return the expected query if the search context does have R&A enabled" in {
      val domainSet = DomainSet(Set(domains(2), domains(3)), Some(domains(3)))
      val query = docQuery.raSearchContextQuery(domainSet).get
      val actual = JsonReader.fromString(query.toString)
      val approved = """{"term": {"approving_domain_ids": {"value": 3, "boost": 1.0}}}"""
      val rejected = """{"term": {"rejecting_domain_ids": {"value": 3, "boost": 1.0}}}"""
      val pending = """{"term": {"pending_domain_ids": {"value": 3, "boost": 1.0}}}"""
      val expected = JsonReader.fromString(s"""
      {
          "bool" : {
              "should" : [$approved, $rejected, $pending],
              $queryDefaults
          }
      }
      """)
      actual should be(expected)
    }
  }

  "the searchContextQuery" should {
    "return None if there is no search context" in {
      val domainSet = DomainSet(Set(domains(2)), None)
      docQuery.searchContextQuery(domainSet) should be(None)
    }

    "return None if the search context has neither R&A or VM enabled" in {
      val domainSet = DomainSet(Set(domains(1)), Some(domains(0)))
      docQuery.searchContextQuery(domainSet) should be(None)
    }

    "return the expected query if the search context has both R&A and VM enabled" in {
      val domainSet = DomainSet(Set(domains(2), domains(3)), Some(domains(3)))
      val query = docQuery.searchContextQuery(domainSet).get
      val actual = JsonReader.fromString(query.toString)
      val vmQuery = docQuery.vmSearchContextQuery(domainSet).get.toString()
      val raQuery = docQuery.raSearchContextQuery(domainSet).get.toString()
      val expected = JsonReader.fromString(s"""
      {
          "bool": {
              "must": [$vmQuery, $raQuery],
              $queryDefaults
          }
      }
      """)
      actual should be(expected)
    }
  }

  "the domainSetQuery" should {
    "return a basic domain query if there is no search context" in {
      val domainSet = DomainSet(Set(domains(2)), None)
      val query = docQuery.domainSetQuery(domainSet)
      val actual = JsonReader.fromString(query.toString)
      val domainQuery = JsonReader.fromString(docQuery.domainIdQuery(Set(2)).toString)
      val expected = JsonReader.fromString(s"""
      {
          "bool": {
              "must": [$domainQuery],
              $queryDefaults
          }
      }
      """)
      actual should be(expected)
    }

    "return a basic domain query if the search context has neither R&A or VM enabled" in {
      val domainSet = DomainSet(Set(domains(1)), Some(domains(0)))
      val query = docQuery.domainSetQuery(domainSet)
      val actual = JsonReader.fromString(query.toString)
      val domainQuery = JsonReader.fromString(docQuery.domainIdQuery(Set(1)).toString)
      val expected = JsonReader.fromString(s"""
      {
          "bool": {
              "must": [$domainQuery],
              $queryDefaults
          }
      }
      """)
      actual should be(expected)
    }

    "return the expected query if the search context has both R&A and VM enabled" in {
      val domainSet = DomainSet(Set(domains(2), domains(3)), Some(domains(2)))
      val query = docQuery.domainSetQuery(domainSet)
      val actual = JsonReader.fromString(query.toString)
      val domainQuery = JsonReader.fromString(docQuery.domainIdQuery(Set(2, 3)).toString)
      val contextQuery = JsonReader.fromString(docQuery.searchContextQuery(domainSet).get.toString)
      val expected = JsonReader.fromString(s"""
      {
          "bool": {
              "must": [$domainQuery, $contextQuery],
              $queryDefaults
          }
      }
      """)
      actual should be(expected)
    }
  }

  "the moderationStatusQuery for approved" should {
    val defaultQuery = j"""{"term": {"is_default_view": {"value": true, "boost": 1.0}}}"""
    val approvedQuery = j"""{"term": {"moderation_status": {"value": "approved", "boost": 1.0}}}"""

    "include only default/approved views if there are no unmoderated domains" in {
      val domainSet = DomainSet(Set(domains(1), domains(3)))
      val query = docQuery.moderationStatusQuery(ApprovalStatus.approved, domainSet)
      val actual = JsonReader.fromString(query.toString)
      val fromNoDomain = j"""{"terms": {"socrata_id.domain_id": [], "boost": 1.0}}"""
      val expected = JsonReader.fromString(s"""
      {
          "bool": {
              "should": [$defaultQuery, $approvedQuery, $fromNoDomain],
              $queryDefaults
          }
      }
      """)
      actual should be(expected)
    }

    "include default/approved views + those from unmoderated domain if there are some unmoderated domains" in {
      val domainSet = DomainSet(Set(domains(0), domains(2), domains(3)))
      val query = docQuery.moderationStatusQuery(ApprovalStatus.approved, domainSet)
      val actual = JsonReader.fromString(query.toString)
      val fromUnmoderatedDomain = """{"terms": {"socrata_id.domain_id": [0, 2], "boost": 1.0}}"""
      val expected = JsonReader.fromString(s"""
      {
          "bool": {
              "should": [$defaultQuery, $approvedQuery, $fromUnmoderatedDomain],
              $queryDefaults
          }
      }
      """)
      actual should be(expected)
    }
  }

  "the moderationStatusFilter for rejected" should {
    val rejectedQuery = j"""{"term": {"moderation_status": {"value": "rejected", "boost": 1.0}}}"""
    val beDerived = j"""{"term": {"is_default_view": {"value": false, "boost": 1.0}}}"""

    "return the expected document-eliminating query if there are no moderated domains" in {
      val domainSet = DomainSet(Set(domains(0), domains(2)))
      val query = docQuery.moderationStatusQuery(ApprovalStatus.rejected, domainSet)
      val actual = JsonReader.fromString(query.toString)
      val fromNoDomain = j"""{"terms": {"socrata_id.domain_id": [], "boost": 1.0}}"""
      val expected = JsonReader.fromString(s"""
      {
          "bool": {
              "must": [$fromNoDomain, $beDerived, $rejectedQuery],
              $queryDefaults
          }
      }
      """)
      actual should be(expected)
    }

    "return rejected views from moderated domain if there are some moderated domains" in {
      val domainSet = DomainSet(Set(domains(1), domains(2), domains(3)))
      val query = docQuery.moderationStatusQuery(ApprovalStatus.rejected, domainSet)
      val actual = JsonReader.fromString(query.toString)
      val fromModeratedDomain = j"""{"terms": {"socrata_id.domain_id": [1, 3], "boost": 1.0}}"""
      val expected = JsonReader.fromString(s"""
      {
          "bool": {
              "must": [$fromModeratedDomain, $beDerived, $rejectedQuery],
              $queryDefaults
          }
      }
      """)
      actual should be(expected)
    }
  }

  "the moderationStatusQuery for pending" should {
    val pendingQuery = j"""{"term": {"moderation_status": {"value": "pending", "boost": 1.0}}}"""
    val beDerived = j"""{"term": {"is_default_view": {"value": false, "boost": 1.0}}}"""

    "return the expected document-eliminating query if there are no moderated domains" in {
      val domainSet = DomainSet(Set(domains(0), domains(2)))
      val query = docQuery.moderationStatusQuery(ApprovalStatus.pending, domainSet)
      val actual = JsonReader.fromString(query.toString)
      val fromNoDomain = """{"terms": {"socrata_id.domain_id": [], "boost": 1.0}}"""
      val expected = JsonReader.fromString(s"""
      {
          "bool": {
              "must": [$fromNoDomain, $beDerived, $pendingQuery],
              $queryDefaults
          }
      }
      """)
      actual should be(expected)
    }

    "return rejected views from moderated domain if there are some moderated domains" in {
      val domainSet = DomainSet(Set(domains(1), domains(2), domains(3)))
      val query = docQuery.moderationStatusQuery(ApprovalStatus.pending, domainSet)
      val actual = JsonReader.fromString(query.toString)
      val fromModeratedDomain = """{"terms": {"socrata_id.domain_id": [1, 3], "boost": 1.0}}"""
      val expected = JsonReader.fromString(s"""
      {
          "bool": {
              "must": [$fromModeratedDomain, $beDerived, $pendingQuery],
              $queryDefaults
          }
      }
      """)
      actual should be(expected)
    }
  }

  "the datalensStatusQuery" should {
    val datalensQuery = j"""{"terms": {"datatype": ["datalens", "datalens_chart", "datalens_map"], "boost": 1.0}}"""

    "include all views that are not unapproved datalens if looking for approved" in {
      val query = docQuery.datalensStatusQuery(ApprovalStatus.approved)
      val actual = JsonReader.fromString(query.toString)
      val unApprovedQuery = JsonReader.fromString(s"""
      {
          "bool": {
              "must_not": [
                  {"term": {"moderation_status": {"value": "approved", "boost": 1.0}}}
              ],
              $queryDefaults
          }
      }
      """)
      val expected = JsonReader.fromString(s"""
      {
          "bool": {
              "must_not": [
                  {
                      "bool": {
                          "must": [$datalensQuery, $unApprovedQuery],
                          $queryDefaults
                      }
                  }
              ],
              $queryDefaults
          }
      }
      """)
      actual should be(expected)
    }

    "include only rejected datalens if looking for rejected" in {
      val query = docQuery.datalensStatusQuery(ApprovalStatus.rejected)
      val actual = JsonReader.fromString(query.toString)
      val rejectedQuery = """{"term": {"moderation_status": {"value": "rejected", "boost": 1.0}}}"""
      val expected = JsonReader.fromString(s"""
      {
          "bool": {
              "must": [$datalensQuery, $rejectedQuery],
              $queryDefaults
          }
      }
      """)
      actual should be(expected)
    }

    "include only pending datalens if looking for pending" in {
      val query = docQuery.datalensStatusQuery(ApprovalStatus.pending)
      val actual = JsonReader.fromString(query.toString)
      val pendingQuery = """{"term": {"moderation_status": {"value": "pending", "boost": 1.0}}}"""
      val expected = JsonReader.fromString(s"""
      {
          "bool": {
              "must": [$datalensQuery, $pendingQuery],
              $queryDefaults
          }
      }
      """)
      actual should be(expected)
    }
  }

  "the raStatusQuery" should {
    val rejected = j"""{"term": {"is_rejected_by_parent_domain": {"value": true, "boost": 1.0}}}"""
    val pending = j"""{"term": {"is_pending_on_parent_domain": {"value": true, "boost": 1.0}}}"""
    val approved =  j"""{"term": {"is_pending_on_parent_domain": {"value": true, "boost": 1.0}}}"""
    val fromCustomerDomainsWithRA = j"""{"terms": {"socrata_id.domain_id": [4, 3, 2], "boost": 1.0}}"""

    "return the expected query when looking for rejected regardless of context" in {
      val status = ApprovalStatus.rejected
      val domainSetNoContext = DomainSet((0 to 4).map(domains(_)).toSet, None)
      val domainSetRaDisabledContext = domainSetNoContext.copy(searchContext = Some(domains(0)))
      val domainSetRaEnabledContext = domainSetNoContext.copy(searchContext = Some(domains(2)))
      val expected = JsonReader.fromString(s"""
      {
          "bool": {
              "must": [$fromCustomerDomainsWithRA, $rejected],
              $queryDefaults
          }
      }
      """)

      val queryNoContext = JsonReader.fromString(docQuery.raStatusQuery(status, domainSetNoContext).toString)
      val queryRaDisabledContext = JsonReader.fromString(docQuery.raStatusQuery(status, domainSetRaDisabledContext).toString)
      val queryRaEnabledContext = JsonReader.fromString(docQuery.raStatusQuery(status, domainSetRaEnabledContext).toString)

      queryNoContext should be(expected)
      queryRaDisabledContext should be(expected)
      queryRaEnabledContext should be(expected)
    }

    "return the expected query when looking for pending regardless of context" in {
      val status = ApprovalStatus.pending
      val domainSetNoContext = DomainSet((0 to 4).map(domains(_)).toSet, None)
      val domainSetRaDisabledContext = domainSetNoContext.copy(searchContext = Some(domains(0)))
      val domainSetRaEnabledContext = domainSetNoContext.copy(searchContext = Some(domains(2)))
      val expected = JsonReader.fromString(s"""
      {
          "bool": {
              "must": [$fromCustomerDomainsWithRA, $pending],
              $queryDefaults
          }
      }
      """)

      val queryNoContext = JsonReader.fromString(docQuery.raStatusQuery(status, domainSetNoContext).toString)
      val queryRaDisabledContext = JsonReader.fromString(docQuery.raStatusQuery(status, domainSetRaDisabledContext).toString)
      val queryRaEnabledContext = JsonReader.fromString(docQuery.raStatusQuery(status, domainSetRaEnabledContext).toString)

      queryNoContext should be(expected)
      queryRaDisabledContext should be(expected)
      queryRaEnabledContext should be(expected)
    }

    "return the expected query when looking for approved regardless of context" in {
      val status = ApprovalStatus.approved
      val domainSetNoContext = DomainSet((0 to 4).map(domains(_)).toSet, None)
      val domainSetRaDisabledContext = domainSetNoContext.copy(searchContext = Some(domains(0)))
      val domainSetRaEnabledContext = domainSetNoContext.copy(searchContext = Some(domains(2)))
      val approvedByParent = j"""{"term": {"is_approved_by_parent_domain": {"value": true, "boost": 1.0}}}"""
      val fromRADisabledDomain = j"""{"terms": {"socrata_id.domain_id": [1, 0], "boost": 1.0}}"""
      val expected = JsonReader.fromString(s"""
      {
          "bool": {
              "should": [$fromRADisabledDomain, $approvedByParent],
              $queryDefaults
          }
      }
      """)

      val queryNoContext = JsonReader.fromString(docQuery.raStatusQuery(status, domainSetNoContext).toString)
      val queryRaDisabledContext = JsonReader.fromString(docQuery.raStatusQuery(status, domainSetRaDisabledContext).toString)
      val queryRaEnabledContext = JsonReader.fromString(docQuery.raStatusQuery(status, domainSetRaEnabledContext).toString)

      queryNoContext should be(expected)
      queryRaDisabledContext should be(expected)
      queryRaEnabledContext should be(expected)
    }
  }

  "the raStatusOnContextQuery" should {
    val someDomains = (0 to 4).map(domains(_)).toSet
    val raEnabledContext = Some(domains(2))
    val raDisabledContext = Some(domains(0))

    "return None if there is no context, regardless of status" in {
      val domainSet = DomainSet(someDomains, None)
      val approvedQuery = docQuery.raStatusOnContextQuery(ApprovalStatus.approved, domainSet)
      val rejectedQuery = docQuery.raStatusOnContextQuery(ApprovalStatus.rejected, domainSet)
      val pendingQuery = docQuery.raStatusOnContextQuery(ApprovalStatus.pending, domainSet)

      approvedQuery should be(None)
      rejectedQuery should be(None)
      pendingQuery should be(None)
    }

    "return None if the context doesn't have R&A, regardless of status" in {
      val domainSet = DomainSet(someDomains, raDisabledContext)
      val approvedQuery = docQuery.raStatusOnContextQuery(ApprovalStatus.approved, domainSet)
      val rejectedQuery = docQuery.raStatusOnContextQuery(ApprovalStatus.rejected, domainSet)
      val pendingQuery = docQuery.raStatusOnContextQuery(ApprovalStatus.pending, domainSet)

      approvedQuery should be(None)
      rejectedQuery should be(None)
      pendingQuery should be(None)
    }

    "return the expected query when looking for approved on a search context with R&A" in {
      val domainSet = DomainSet(someDomains, raEnabledContext)
      val query = docQuery.raStatusOnContextQuery(ApprovalStatus.approved, domainSet)
      val actual = JsonReader.fromString(query.get.toString)
      val expected = j"""{"term": {"approving_domain_ids": {"value": 2, "boost": 1.0}}}"""
      actual should be(expected)
    }

    "return the expected query when looking for rejected on a search context with R&A" in {
      val domainSet = DomainSet(someDomains, raEnabledContext)
      val expected = j"""{"term": {"rejecting_domain_ids": {"value": 2, "boost": 1.0}}}"""
      val query = docQuery.raStatusOnContextQuery(ApprovalStatus.rejected, domainSet)
      val actual = JsonReader.fromString(query.get.toString)
      actual should be(expected)
    }

    "return the expected query when looking for pending on a search context with R&A" in {
      val domainSet = DomainSet(someDomains, raEnabledContext)
      val expected = j"""{"term": {"pending_domain_ids": {"value": 2, "boost": 1.0}}}"""
      val query = docQuery.raStatusOnContextQuery(ApprovalStatus.pending, domainSet)
      val actual = JsonReader.fromString(query.get.toString)
      actual should be(expected)
    }
  }

  "the approvalStatusFilter" should {
    val someDomains = (0 to 4).map(domains(_)).toSet
    val defaultQuery = j"""{"term": {"is_default_view": {"value": true}}}"""
    val approvedQuery = j"""{"term": {"moderation_status": {"value": "approved"}}}"""
    val defaultOrApprovedQuery = JsonReader.fromString(s"""
    {
        "bool": {
            "should": [$defaultQuery, $approvedQuery],
            $queryDefaults
        }
    }
    """)
    val fromModeratedDomain = j"""{"terms": {"socrata_id.domain_id": [1, 3], "boost": 1.0}}"""
    val fromUnmoderatedDomain = j"""{"terms": {"socrata_id.domain_id": [0, 2], "boost": 1.0}}"""
    val fromNoDomain = j"""{"terms": {"socrata_id.domain_id": [], "boost": 1.0}}"""
    val defaultOrApprovedQueryForUnmoderatedContext = JsonReader.fromString(s"""
    {
        "bool": {
            "should": [$defaultQuery, $approvedQuery, $fromNoDomain],
            $queryDefaults
        }
    }
    """)
    val moderatedDomains = Set(domains(1), domains(3))
    val unmoderatedDomains = Set(domains(0), domains(2))

    "return the expected query for approved views if the status is approved and we include the context" in {
      val domainSet = DomainSet(someDomains, Some(domains(3)))
      val beModApproved = JsonReader.fromString(docQuery.moderationStatusQuery(ApprovalStatus.approved, domainSet).toString)
      val beDatalensApproved = JsonReader.fromString(docQuery.datalensStatusQuery(ApprovalStatus.approved).toString)
      val beRAApproved = JsonReader.fromString(docQuery.raStatusQuery(ApprovalStatus.approved, domainSet).toString)
      val beRAApprovedOnContext = JsonReader.fromString(docQuery.raStatusOnContextQuery(ApprovalStatus.approved, domainSet).get.toString)
      val expected = JsonReader.fromString(s"""
      {
          "bool": {
              "must": [$beModApproved, $beDatalensApproved, $beRAApproved, $beRAApprovedOnContext],
              $queryDefaults
          }
      }
      """)

      val query = docQuery.approvalStatusQuery(ApprovalStatus.approved, domainSet, includeContextApproval = true)
      val actual = JsonReader.fromString(query.toString)
      actual should be(expected)
    }

    "return the expected query for approved views if the status is approved and we exclude the context" in {
      val domainSet = DomainSet(someDomains, Some(domains(3)))
      val beModApproved = JsonReader.fromString(docQuery.moderationStatusQuery(ApprovalStatus.approved, domainSet).toString)
      val beDatalensApproved = JsonReader.fromString(docQuery.datalensStatusQuery(ApprovalStatus.approved).toString)
      val beRAApproved = JsonReader.fromString(docQuery.raStatusQuery(ApprovalStatus.approved, domainSet).toString)
      val expected = JsonReader.fromString(s"""
      {
          "bool": {
              "must": [$beModApproved, $beDatalensApproved, $beRAApproved],
              $queryDefaults
          }
      }
      """)

      val query = docQuery.approvalStatusQuery(ApprovalStatus.approved, domainSet, includeContextApproval = false)
      val actual = JsonReader.fromString(query.toString)
      actual should be(expected)
    }

    "return the expected query for rejected views if the status is rejected and we include the context" in {
      val domainSet = DomainSet(someDomains, Some(domains(3)))
      val beModRejected = JsonReader.fromString(docQuery.moderationStatusQuery(ApprovalStatus.rejected, domainSet).toString)
      val beDatalensRejected = JsonReader.fromString(docQuery.datalensStatusQuery(ApprovalStatus.rejected).toString)
      val beRARejected = JsonReader.fromString(docQuery.raStatusQuery(ApprovalStatus.rejected, domainSet).toString)
      val beRARejectedOnContext = JsonReader.fromString(docQuery.raStatusOnContextQuery(ApprovalStatus.rejected, domainSet).get.toString)
      val expected = JsonReader.fromString(s"""
      {
          "bool": {
              "should": [$beModRejected, $beDatalensRejected, $beRARejected, $beRARejectedOnContext],
              $queryDefaults
          }
      }
      """)

      val query = docQuery.approvalStatusQuery(ApprovalStatus.rejected, domainSet, includeContextApproval = true)
      val actual = JsonReader.fromString(query.toString)
      actual should be(expected)
    }

    "return the expected query for rejected views if the status is rejected and we exclude the context" in {
      val domainSet = DomainSet(someDomains, Some(domains(3)))
      val beModRejected = JsonReader.fromString(docQuery.moderationStatusQuery(ApprovalStatus.rejected, domainSet).toString)
      val beDatalensRejected = JsonReader.fromString(docQuery.datalensStatusQuery(ApprovalStatus.rejected).toString)
      val beRARejected = JsonReader.fromString(docQuery.raStatusQuery(ApprovalStatus.rejected, domainSet).toString)
      val expected = JsonReader.fromString(s"""
      {
          "bool": {
              "should": [$beModRejected, $beDatalensRejected, $beRARejected],
              $queryDefaults
          }
      }
      """)

      val query = docQuery.approvalStatusQuery(ApprovalStatus.rejected, domainSet, includeContextApproval = false)
      val actual = JsonReader.fromString(query.toString)
      actual should be(expected)
    }

    "return the expected query for pending views if the status is pending and we include the context" in {
      val domainSet = DomainSet(someDomains, Some(domains(3)))
      val beModPending = JsonReader.fromString(docQuery.moderationStatusQuery(ApprovalStatus.pending, domainSet).toString)
      val beDatalensPending = JsonReader.fromString(docQuery.datalensStatusQuery(ApprovalStatus.pending).toString)
      val beRAPending = JsonReader.fromString(docQuery.raStatusQuery(ApprovalStatus.pending, domainSet).toString)
      val beRAPendingOnContext = JsonReader.fromString(docQuery.raStatusOnContextQuery(ApprovalStatus.pending, domainSet).get.toString)
      val expected = JsonReader.fromString(s"""
      {
          "bool": {
              "should": [$beModPending, $beDatalensPending, $beRAPending, $beRAPendingOnContext],
              $queryDefaults
          }
      }
      """)

      val query = docQuery.approvalStatusQuery(ApprovalStatus.pending, domainSet, includeContextApproval = true)
      val actual = JsonReader.fromString(query.toString)
      actual should be(expected)
    }

    "return the expected query for pending views if the status is pending and we exclude the context" in {
      val domainSet = DomainSet(someDomains, Some(domains(3)))
      val beModPending = JsonReader.fromString(docQuery.moderationStatusQuery(ApprovalStatus.pending, domainSet).toString)
      val beDatalensPending = JsonReader.fromString(docQuery.datalensStatusQuery(ApprovalStatus.pending).toString)
      val beRAPending = JsonReader.fromString(docQuery.raStatusQuery(ApprovalStatus.pending, domainSet).toString)
      val expected = JsonReader.fromString(s"""
      {
          "bool": {
              "should": [$beModPending, $beDatalensPending, $beRAPending],
              $queryDefaults
          }
      }
      """)

      val query = docQuery.approvalStatusQuery(ApprovalStatus.pending, domainSet, includeContextApproval = false)
      val actual = JsonReader.fromString(query.toString)
      actual should be(expected)
    }
  }

  "the socrataCategoriesQuery" should {
    "return the expected query when there are categories given" in {
      val query = docQuery.socrataCategoriesQuery(Set("cats", "dogs"))
      val actual = JsonReader.fromString(query.toString)
      val expected = JsonReader.fromString(s"""
      {
          "bool": {
              "should": [
                  {
                      "nested": {
                          "query": {
                              "terms": {
                                  "animl_annotations.categories.name.raw": ["cats", "dogs"],
                                  "boost": 1.0
                              }
                          },
                          "path": "animl_annotations.categories",
                          "ignore_unmapped": false,
                          "score_mode": "avg",
                          "boost" : 1.0
                      }
                  }
              ],
              $queryDefaults
          }
      }
      """)
      actual should be(expected)
    }
  }

  "the socrataTagsQuery" should {
    "return the expected filter when there are tags given" in {
      val query = docQuery.socrataTagsQuery(Set("cats", "dogs"))
      val actual = JsonReader.fromString(query.toString)
      val expected = JsonReader.fromString(s"""
      {
          "bool": {
              "should": [
                  {
                      "nested": {
                          "query": {
                              "terms": {
                                  "animl_annotations.tags.name.raw": ["cats", "dogs"],
                                  "boost": 1.0
                              }
                          },
                          "path": "animl_annotations.tags",
                          "ignore_unmapped": false,
                          "score_mode": "avg",
                          "boost" : 1.0
                      }
                  }
              ],
              $queryDefaults
          }
      }
      """)
      actual should be(expected)
    }
  }

  "the categoryQuery" should {
    "return None if we aren't searching over domains" in {
      val docQueryNoContext = docQuery.categoryQuery(Set("cats", "dogs"), context = None)
      val docQueryWithContext = docQuery.categoryQuery(Set("cats", "dogs"), context = Some(domains(0)))
      docQueryNoContext should be(None)
      docQueryWithContext should be(None)
    }

    "return the customer category query if we are searching over domains and we have a context" in {
      val categories = Set("cats", "dogs")
      val query = docQueryForDomainSearch.categoryQuery(categories, context = Some(domains(0)))
      val expectedQuery = JsonReader.fromString(docQueryForDomainSearch.customerCategoriesQuery(categories).toString)
      val actualQuery = JsonReader.fromString(query.get.toString)
      actualQuery should be(expectedQuery)
    }

    "return the socrata category query if we are searching over domains and we do not have a context" in {
      val categories = Set("cats", "dogs")
      val query = docQueryForDomainSearch.categoryQuery(categories, context = None)
      val expectedQuery = JsonReader.fromString(docQueryForDomainSearch.socrataCategoriesQuery(categories).toString)
      val actualQuery = JsonReader.fromString(query.get.toString)
      actualQuery should be(expectedQuery)
    }
  }

  "the tagQuery" should {
    "return None if we aren't searching over domains" in {
      val docQueryNoContext = docQuery.tagQuery(Set("cats", "dogs"), context = None)
      val docQueryWithContext = docQuery.tagQuery(Set("cats", "dogs"), context = Some(domains(0)))
      docQueryNoContext should be(None)
      docQueryWithContext should be(None)
    }

    "return the customer tag query if we are searching over domains and we have a context" in {
      val tags = Set("cats", "dogs")
      val query = docQueryForDomainSearch.tagQuery(tags, context = Some(domains(0)))
      val expectedQuery = JsonReader.fromString(docQueryForDomainSearch.customerTagsQuery(tags).toString)
      val actualQuery = JsonReader.fromString(query.get.toString)
      actualQuery should be(expectedQuery)
    }

    "return the socrata tag query if we are searching over domains and we do not have a context" in {
      val tags = Set("cats", "dogs")
      val query = docQueryForDomainSearch.tagQuery(tags, context = None)
      val expectedQuery = JsonReader.fromString(docQueryForDomainSearch.socrataTagsQuery(tags).toString)
      val actualQuery = JsonReader.fromString(query.get.toString)
      actualQuery should be(expectedQuery)
    }
  }

  "the publicQuery" should {
    "return the expected query when no params are passed" in {
      val query = docQuery.publicQuery()
      val actual = JsonReader.fromString(query.toString)
      val expected = j"""{"term": {"is_public": {"value": true, "boost": 1.0}}}"""
      actual should be(expected)
    }

    "return the expected query when a 'true' param passed" in {
      val query = docQuery.publicQuery(true)
      val actual = JsonReader.fromString(query.toString)
      val expected = j"""{"term": {"is_public": {"value": true, "boost": 1.0}}}"""
      actual should be(expected)
    }

    "return the expected query when a 'false' param passed" in {
      val query = docQuery.publicQuery(false)
      val actual = JsonReader.fromString(query.toString)
      val expected = j"""{"term": {"is_public": {"value": false, "boost": 1.0}}}"""
      actual should be(expected)
    }
  }

  "the publishedQuery" should {
    "return the expected query when no params are passed" in {
      val query = docQuery.publishedQuery()
      val actual = JsonReader.fromString(query.toString)
      val expected = j"""{"term": {"is_published": {"value": true, "boost": 1.0}}}"""
      actual should be(expected)
    }

    "return the expected query when a 'true' param passed" in {
      val query = docQuery.publishedQuery(true)
      val actual = JsonReader.fromString(query.toString)
      val expected = j"""{"term": {"is_published": {"value": true, "boost": 1.0}}}"""
      actual should be(expected)
    }

    "return the expected query when a 'false' param passed" in {
      val query = docQuery.publishedQuery(false)
      val actual = JsonReader.fromString(query.toString)
      val expected = j"""{"term": {"is_published": {"value": false, "boost": 1.0}}}"""
      actual should be(expected)
    }
  }

  "the searchParamsQuery" should {
    "throw an unauthorizedError if there is no authenticated user looking for what is shared to another user" in {
      val searchParams = SearchParamSet(sharedTo = Some("ro-bear"))
      val user = None
      an[UnauthorizedError] should be thrownBy {
        docQuery.searchParamsQuery(searchParams, user, DomainSet())
      }
    }

    "throw an unauthorizedError if there is an authenticated user is looking for what is shared to another user" in {
      val searchParams = SearchParamSet(sharedTo = Some("ro-bear"))
      val user = Some(AuthedUser("anna-belle", domains(0), roleName = Some("administrator")))
      an[UnauthorizedError] should be thrownBy {
        docQuery.searchParamsQuery(searchParams, user, DomainSet())
      }
    }

    "return the expected query" in {
      val searchParams = SearchParamSet(
        searchQuery = SimpleQuery("search query terms"),
        domains = Some(Set("www.example.com", "test.example.com", "socrata.com")),
        searchContext = Some("www.search.me"),
        domainMetadata = Some(Set(("key", "value"))),
        categories = Some(Set("Social Services", "Environment", "Housing & Development")),
        tags = Some(Set("taxi", "art", "clowns")),
        datatypes = Some(Set("dataset")),
        user = Some("anna-belle"),
        attribution = Some("org"),
        parentDatasetId = Some("parent-id"),
        ids = Some(Set("id-one", "id-two")),
        license = Some("GNU GPL")
      )
      val query = docQuery.searchParamsQuery(searchParams, None, DomainSet()).get
      val actual = JsonReader.fromString(query.toString)
      val expected = JsonReader.fromString(s"""
      {
          "bool": {
              "must": [
                  {"terms": {"datatype": ["dataset"], "boost": 1.0}},
                  {"term": {"owner.id": {"value": "anna-belle", "boost": 1.0}}},
                  {"term": {"attribution.raw": {"value": "org", "boost": 1.0}}},
                  {"term": {"socrata_id.parent_dataset_id": {"value": "parent-id", "boost": 1.0}}},
                  {"ids": {"type": ["document"], "values": ["id-one", "id-two"], "boost": 1.0}},
                  {
                      "bool": {
                          "should": [
                              {
                                  "nested": {
                                      "query": {
                                          "bool": {
                                              "must": [
                                                  {
                                                      "terms": {
                                                          "customer_metadata_flattened.key.raw": ["key"],
                                                          "boost": 1.0
                                                      }
                                                  },
                                                  {
                                                      "terms": {
                                                          "customer_metadata_flattened.value.raw": ["value"],
                                                          "boost": 1.0
                                                      }
                                                  }
                                              ],
                                              $queryDefaults
                                          }
                                      },
                                      "path": "customer_metadata_flattened",
                                      "ignore_unmapped": false,
                                      "score_mode": "avg",
                                      "boost": 1.0
                                  }
                              }
                          ],
                          $queryDefaults
                      }
                  },
                  {"term": {"license": {"value": "GNU GPL", "boost": 1.0}}}
              ],
              $queryDefaults
          }
      }
      """)

      actual should be(expected)
    }
  }

  "the anonymousQuery" should {
    "return the expected query" in {
      val domainSet = DomainSet((0 to 2).map(domains(_)).toSet, Some(domains(2)))
      val public = JsonReader.fromString(docQuery.publicQuery(public = true).toString)
      val published = JsonReader.fromString(docQuery.publishedQuery(published = true).toString)
      val approved = JsonReader.fromString(docQuery.approvalStatusQuery(ApprovalStatus.approved, domainSet).toString)
      val unhidden = JsonReader.fromString(docQuery.hiddenFromCatalogQuery(false).toString)
      val expected = JsonReader.fromString(s"""
      {
          "bool": {
              "must": [$public, $published, $approved, $unhidden],
              $queryDefaults
          }
      }
      """)
      val query = docQuery.anonymousQuery(domainSet)
      val actual = JsonReader.fromString(query.toString)
      actual should be(expected)
    }
  }

  "the ownedOrSharedQuery" should {
    "return the expected query" in {
      val user = AuthedUser("mooks", domains(0))
      val query = docQuery.ownedOrSharedQuery(user)
      val actual = JsonReader.fromString(query.toString)
      val expected = JsonReader.fromString(s"""
      {
          "bool": {
              "should": [
                  {"term": {"owner.id": {"value": "mooks", "boost": 1.0}}},
                  {"term": {"shared_to": {"value": "mooks", "boost": 1.0}}}
              ],
              $queryDefaults
          }
      }
      """)
      actual should be(expected)
    }
  }

  "the authQuery" should {
    val someDomains = (0 to 4).map(domains(_)).toSet
    val contextWithRA = domains(3)

    "return None for super admins" in {
      val user = AuthedUser("mooks", domains(0), flags = Some(List("admin")))
      val domainSet = DomainSet(someDomains, Some(contextWithRA))
      val query = docQuery.authQuery(Some(user), domainSet)
      query should be(None)
    }

    "return anon views (disregarding context), personal views and within-domains views for users who can view everything and do have an authenticating domain" in {
      val user = AuthedUser("mooks", contextWithRA, roleName = Some("publisher"))
      val domainSet = DomainSet(someDomains, Some(contextWithRA))
      val query = docQuery.authQuery(Some(user), domainSet)
      val actual = JsonReader.fromString(query.get.toString)
      val anonQuery = JsonReader.fromString(docQuery.anonymousQuery(domainSet, includeContextApproval = false).toString)
      val personalQuery = JsonReader.fromString(docQuery.ownedOrSharedQuery(user).toString)
      val withinDomainQuery = JsonReader.fromString(docQuery.domainIdQuery(Set(3)).toString)
      val expected = JsonReader.fromString(s"""
      {
          "bool": {
              "should": [$personalQuery, $anonQuery, $withinDomainQuery],
              $queryDefaults
          }
      }
      """)
      actual should be(expected)
    }

    "return anon (acknowleding context) and personal views for users who have logged in but cannot view everything" in {
      val user = AuthedUser("mooks", contextWithRA, roleName = Some("editor"))
      val domainSet = DomainSet(someDomains, Some(contextWithRA))
      val query = docQuery.authQuery(Some(user), domainSet)
      val actual = JsonReader.fromString(query.get.toString)
      val anonQuery = JsonReader.fromString(docQuery.anonymousQuery(domainSet, includeContextApproval = true).toString)
      val personalQuery = JsonReader.fromString(docQuery.ownedOrSharedQuery(user).toString)
      val expected = JsonReader.fromString(s"""
      {
          "bool": {
              "should": [$personalQuery, $anonQuery],
              $queryDefaults
          }
      }
      """)
      actual should be(expected)
    }

    "return anon (acknowleding context) views for anonymous users" in {
      val domainSet = DomainSet(someDomains, Some(contextWithRA))
      val query = docQuery.authQuery(None, domainSet)
      val actual = JsonReader.fromString(query.get.toString)
      val anonQuery = JsonReader.fromString(docQuery.anonymousQuery(domainSet, includeContextApproval = true).toString)

      actual should be(anonQuery)
    }
  }

  "DocumentQueries: simpleQuery" should {
    "return the expected query when nothing but a query is given" in {
      val actual = JsonReader.fromString(
        DocumentQuery().simpleQuery(queryString, ScoringParamSet(), None).toString)

      val expected = JsonReader.fromString(s"""
      {
          "bool": {
              "must": [
                  {
                      "multi_match": {
                          "query": "snuffy",
                          "fields": [
                              "fts_analyzed^1.0",
                              "fts_raw^1.0"
                          ],
                          "type": "cross_fields",
                          "operator": "OR",
                          "slop": 0,
                          "prefix_length": 0,
                          "max_expansions": 50,
                          "lenient": false,
                          "zero_terms_query": "NONE",
                          "boost": 1.0
                      }
                  }
              ],
              "should": [
                  {
                      "multi_match": {
                          "query": "snuffy",
                          "fields": [
                              "fts_analyzed^1.0",
                              "fts_raw^1.0"
                          ],
                          "type": "phrase",
                          "operator": "OR",
                          "slop": 0,
                          "prefix_length": 0,
                          "max_expansions": 50,
                          "lenient": false,
                          "zero_terms_query": "NONE",
                          "boost": 1.0
                      }
                  }
              ],
              $queryDefaults
          }
      }
      """)

      actual should be(expected)
    }

    "return the expected query when a query and field boosts are given" in {
      val actual = JsonReader.fromString(
        DocumentQuery().simpleQuery(
          queryString, ScoringParamSet(fieldBoosts = fieldBoosts), None).toString)

      val expected = JsonReader.fromString(s"""
      {
          "bool": {
              "must": [
                  {
                      "multi_match": {
                          "query": "snuffy",
                          "fields": [
                              "fts_analyzed^1.0",
                              "fts_raw^1.0"
                          ],
                          "type": "cross_fields",
                          "operator": "OR",
                          "slop": 0,
                          "prefix_length": 0,
                          "max_expansions": 50,
                          "lenient": false,
                          "zero_terms_query": "NONE",
                          "boost": 1.0
                      }
                  }
              ],
              "should": [
                  {
                      "multi_match": {
                          "query": "snuffy",
                          "fields": [
                              "fts_analyzed^1.0",
                              "fts_raw^1.0",
                              "indexed_metadata.name^2.2"
                          ],
                          "type": "phrase",
                          "operator": "OR",
                          "slop": 0,
                          "prefix_length": 0,
                          "max_expansions": 50,
                          "lenient": false,
                          "zero_terms_query": "NONE",
                          "boost": 1.0
                      }
                  }
              ],
              $queryDefaults
          }
      }
      """)

      actual should be(expected)
    }

    "return the expected query when a query and minShouldMatch are given" in {
      val actual = JsonReader.fromString(
        DocumentQuery().simpleQuery(
          queryString, ScoringParamSet(minShouldMatch = Some("2<-25% 9<-3")), None).toString)

      val expected = JsonReader.fromString(s"""
      {
          "bool": {
              "must": [
                  {
                      "multi_match": {
                          "query": "snuffy",
                          "fields": [
                              "fts_analyzed^1.0",
                              "fts_raw^1.0"
                          ],
                          "type": "cross_fields",
                          "operator": "OR",
                          "slop": 0,
                          "prefix_length": 0,
                          "max_expansions": 50,
                          "minimum_should_match": "2<-25% 9<-3",
                          "lenient": false,
                          "zero_terms_query": "NONE",
                          "boost": 1.0
                      }
                  }
              ],
              "should": [
                  {
                      "multi_match": {
                          "query": "snuffy",
                          "fields": [
                              "fts_analyzed^1.0",
                              "fts_raw^1.0"
                          ],
                          "type": "phrase",
                          "operator": "OR",
                          "slop": 0,
                          "prefix_length": 0,
                          "max_expansions": 50,
                          "lenient": false,
                          "zero_terms_query": "NONE",
                          "boost": 1.0
                      }
                  }
              ],
              $queryDefaults
          }
      }
      """)

      actual should be(expected)
    }

    "return the expected query when a query and slop is given" in {
      val actual = JsonReader.fromString(
        DocumentQuery().simpleQuery(queryString, ScoringParamSet(slop = Some(2)), None).toString)

      val expected = JsonReader.fromString(s"""
      {
          "bool": {
              "must": [
                  {
                      "multi_match": {
                          "query": "snuffy",
                          "fields": [
                              "fts_analyzed^1.0",
                              "fts_raw^1.0"
                          ],
                          "type": "cross_fields",
                          "operator": "OR",
                          "slop": 0,
                          "prefix_length": 0,
                          "max_expansions": 50,
                          "lenient": false,
                          "zero_terms_query": "NONE",
                          "boost": 1.0
                      }
                  }
              ],
              "should": [
                  {
                      "multi_match": {
                          "query": "snuffy",
                          "fields": [
                              "fts_analyzed^1.0",
                              "fts_raw^1.0"
                          ],
                          "type": "phrase",
                          "operator": "OR",
                          "slop": 2,
                          "prefix_length": 0,
                          "max_expansions": 50,
                          "lenient": false,
                          "zero_terms_query": "NONE",
                          "boost": 1.0
                      }
                  }
              ],
              $queryDefaults
          }
      }
      """)

      actual should be(expected)
    }
  }

    "return the expected query when a query, minShouldMatch and slop are specified" in {
      val scoringSet = ScoringParamSet(minShouldMatch = Some("2<-25% 9<-3"), slop = Some(2))

      val actual = JsonReader.fromString(
        DocumentQuery().chooseMatchQuery(SimpleQuery(queryString), scoringSet, None).toString)

      val expected = JsonReader.fromString(s"""
      {
          "bool": {
              "must": [
                  {
                      "multi_match": {
                          "query": "snuffy",
                          "fields": [
                              "fts_analyzed^1.0",
                              "fts_raw^1.0"
                          ],
                          "type": "cross_fields",
                          "operator": "OR",
                          "slop": 0,
                          "prefix_length": 0,
                          "max_expansions": 50,
                          "minimum_should_match" : "2<-25% 9<-3",
                          "lenient": false,
                          "zero_terms_query": "NONE",
                          "boost": 1.0
                      }
                  }
              ],
              "should": [
                  {
                      "multi_match": {
                          "query": "snuffy",
                          "fields": [
                              "fts_analyzed^1.0",
                              "fts_raw^1.0"
                          ],
                          "type": "phrase",
                          "operator": "OR",
                          "slop": 2,
                          "prefix_length": 0,
                          "max_expansions": 50,
                          "lenient": false,
                          "zero_terms_query": "NONE",
                          "boost": 1.0
                      }
                  }
              ],
              $queryDefaults
          }
      }
      """)

      actual should be(expected)
    }

    "produce a query with lots of scoring params" in {
      val actual = JsonReader.fromString(
        DocumentQuery().chooseMatchQuery(
          SimpleQuery("the dog chased the cat"),
          ScoringParamSet(
            fieldBoosts = Map(DescriptionFieldType -> 7.77f, TitleFieldType -> 8.88f),
            minShouldMatch = Some("20%"),
            slop = Some(12)),
          None
        ).toString)

      val expected = JsonReader.fromString(s"""
      {
          "bool": {
              "must": [
                  {
                      "multi_match": {
                          "query": "the dog chased the cat",
                          "fields": [
                              "fts_analyzed^1.0",
                              "fts_raw^1.0"
                          ],
                          "type": "cross_fields",
                          "operator": "OR",
                          "slop": 0,
                          "prefix_length": 0,
                          "max_expansions": 50,
                          "minimum_should_match" : "20%",
                          "lenient": false,
                          "zero_terms_query": "NONE",
                          "boost": 1.0
                      }
                  }
              ],
              "should": [
                  {
                      "multi_match": {
                          "query": "the dog chased the cat",
                          "fields": [
                              "fts_analyzed^1.0",
                              "fts_raw^1.0",
                              "indexed_metadata.description^7.77",
                              "indexed_metadata.name^8.88"
                          ],
                          "type": "phrase",
                          "operator": "OR",
                          "slop": 12,
                          "prefix_length": 0,
                          "max_expansions": 50,
                          "lenient": false,
                          "zero_terms_query": "NONE",
                          "boost": 1.0
                      }
                  }
              ],
              $queryDefaults
          }
      }
      """)

      actual should be (expected)
    }

  "DocumentQuery(): advancedQuery" should {
    "return the expected query when nothing but a query is given" in {
      val actual = JsonReader.fromString(
        DocumentQuery().advancedQuery(queryString, emptyFieldBoosts).toString)

      val expected = JsonReader.fromString(s"""
      {
          "bool": {
              "should": [
                  {
                      "query_string": {
                          "query": "snuffy",
                          "fields": [
                              "fts_analyzed^1.0",
                              "fts_raw^1.0"
                          ],
                          "use_dis_max": true,
                          "tie_breaker": 0.0,
                          "default_operator": "or",
                          "auto_generate_phrase_queries": true,
                          "max_determined_states": 10000,
                          "enable_position_increment": true,
                          "fuzziness": "AUTO",
                          "fuzzy_prefix_length": 0,
                          "fuzzy_max_expansions": 50,
                          "phrase_slop": 0,
                          "escape": false,
                          "split_on_whitespace": true,
                          "boost": 1.0
                      }
                  },
                  {
                      "has_parent": {
                          "query": {
                              "query_string": {
                                  "query": "snuffy",
                                  "fields": [
                                      "cnames^1.0",
                                      "fts_analyzed^1.0",
                                      "fts_raw^1.0"
                                  ],
                                  "use_dis_max": true,
                                  "tie_breaker": 0.0,
                                  "default_operator": "or",
                                  "auto_generate_phrase_queries": true,
                                  "max_determined_states": 10000,
                                  "enable_position_increment": true,
                                  "fuzziness": "AUTO",
                                  "fuzzy_prefix_length": 0,
                                  "fuzzy_max_expansions": 50,
                                  "phrase_slop": 0,
                                  "escape": false,
                                  "split_on_whitespace": true,
                                  "boost": 1.0
                              }
                          },
                          "parent_type": "domain",
                          "score": false,
                          "ignore_unmapped": false,
                          "boost": 1.0
                      }
                  }
              ],
              $queryDefaults
          }
      }
      """)
      actual should be(expected)
    }

    "return the expected query when a query and field boosts are given" in {
      val actual = JsonReader.fromString(
        DocumentQuery().advancedQuery(queryString, fieldBoosts).toString)

      val expected = JsonReader.fromString(s"""
      {
          "bool": {
              "should": [
                  {
                      "query_string": {
                          "query": "snuffy",
                          "fields": [
                              "fts_analyzed^1.0",
                              "fts_raw^1.0",
                              "indexed_metadata.name^2.2"
                          ],
                          "use_dis_max": true,
                          "tie_breaker": 0.0,
                          "default_operator": "or",
                          "auto_generate_phrase_queries": true,
                          "max_determined_states": 10000,
                          "enable_position_increment": true,
                          "fuzziness": "AUTO",
                          "fuzzy_prefix_length": 0,
                          "fuzzy_max_expansions": 50,
                          "phrase_slop": 0,
                          "escape": false,
                          "split_on_whitespace": true,
                          "boost": 1.0
                      }
                  },
                  {
                      "has_parent": {
                          "query": {
                              "query_string": {
                                  "query": "snuffy",
                                  "fields": [
                                      "cnames^1.0",
                                      "fts_analyzed^1.0",
                                      "fts_raw^1.0",
                                      "indexed_metadata.name^2.2"
                                  ],
                                  "use_dis_max": true,
                                  "tie_breaker": 0.0,
                                  "default_operator": "or",
                                  "auto_generate_phrase_queries": true,
                                  "max_determined_states": 10000,
                                  "enable_position_increment": true,
                                  "fuzziness": "AUTO",
                                  "fuzzy_prefix_length": 0,
                                  "fuzzy_max_expansions": 50,
                                  "phrase_slop": 0,
                                  "escape": false,
                                  "split_on_whitespace": true,
                                  "boost": 1.0
                              }
                          },
                          "parent_type": "domain",
                          "score": false,
                          "ignore_unmapped": false,
                          "boost": 1.0
                      }
                  }
              ],
              $queryDefaults
          }
      }
      """)

      actual should be(expected)
    }

    "produce an advanced query with field boosts applied" in {
      val actual = JsonReader.fromString(
        DocumentQuery().advancedQuery(
          "any old query string",
          Map(
            ColumnDescriptionFieldType -> 1.11f,
            ColumnFieldNameFieldType -> 2.22f,
            ColumnNameFieldType -> 3.33f,
            DatatypeFieldType -> 4.44f,
            DescriptionFieldType -> 5.55f,
            TitleFieldType -> 6.66f)
        ).toString)

      val expected = JsonReader.fromString(s"""
      {
          "bool": {
              "should": [
                  {
                      "query_string": {
                          "query": "any old query string",
                          "fields": [
                               "datatype^4.44",
                               "fts_analyzed^1.0",
                               "fts_raw^1.0",
                               "indexed_metadata.columns_description^1.11",
                               "indexed_metadata.columns_field_name^2.22",
                               "indexed_metadata.columns_name^3.33",
                               "indexed_metadata.description^5.55",
                               "indexed_metadata.name^6.66"
                          ],
                          "use_dis_max": true,
                          "tie_breaker": 0.0,
                          "default_operator": "or",
                          "auto_generate_phrase_queries": true,
                          "max_determined_states": 10000,
                          "enable_position_increment": true,
                          "fuzziness": "AUTO",
                          "fuzzy_prefix_length": 0,
                          "fuzzy_max_expansions": 50,
                          "phrase_slop": 0,
                          "escape": false,
                          "split_on_whitespace": true,
                          "boost": 1.0
                      }
                  },
                  {
                      "has_parent": {
                          "query": {
                              "query_string": {
                                  "query": "any old query string",
                                  "fields": [
                                       "cnames^1.0",
                                       "datatype^4.44",
                                       "fts_analyzed^1.0",
                                       "fts_raw^1.0",
                                       "indexed_metadata.columns_description^1.11",
                                       "indexed_metadata.columns_field_name^2.22",
                                       "indexed_metadata.columns_name^3.33",
                                       "indexed_metadata.description^5.55",
                                       "indexed_metadata.name^6.66"
                                  ],
                                  "use_dis_max": true,
                                  "tie_breaker": 0.0,
                                  "default_operator": "or",
                                  "auto_generate_phrase_queries": true,
                                  "max_determined_states": 10000,
                                  "enable_position_increment": true,
                                  "fuzziness": "AUTO",
                                  "fuzzy_prefix_length": 0,
                                  "fuzzy_max_expansions": 50,
                                  "phrase_slop": 0,
                                  "escape": false,
                                  "split_on_whitespace": true,
                                  "boost": 1.0
                              }
                          },
                          "parent_type": "domain",
                          "score": false,
                          "ignore_unmapped": false,
                          "boost": 1.0
                      }
                  }
              ],
              $queryDefaults
          }
      }
      """)

      actual should be (expected)
    }
  }

  "DocumentQuery(): chooseMatchQuery" should {
    "return the expected query when no query is given" in {
      val actual = JsonReader.fromString(
        DocumentQuery().chooseMatchQuery(NoQuery, ScoringParamSet(), None).toString)

      val expected = JsonReader.fromString(s"""
      {
          "bool": {
              "must": [{"match_all": {"boost": 1.0}}],
              $queryDefaults
          }
      }
      """)

      actual should be(expected)
    }

    "return the expected query when a simple query is given" in {
      val actual = JsonReader.fromString(
        DocumentQuery().chooseMatchQuery(
          SimpleQuery(queryString), ScoringParamSet(), None).toString)

      val expected = JsonReader.fromString(s"""
      {
          "bool": {
              "must": [
                  {
                      "multi_match": {
                          "query": "snuffy",
                          "fields": [
                              "fts_analyzed^1.0",
                              "fts_raw^1.0"
                          ],
                          "type": "cross_fields",
                          "operator": "OR",
                          "slop": 0,
                          "prefix_length": 0,
                          "max_expansions": 50,
                          "lenient": false,
                          "zero_terms_query": "NONE",
                          "boost": 1.0
                      }
                  }
              ],
              "should": [
                  {
                      "multi_match": {
                          "query": "snuffy",
                          "fields": [
                              "fts_analyzed^1.0",
                              "fts_raw^1.0"
                          ],
                          "type": "phrase",
                          "operator": "OR",
                          "slop": 0,
                          "prefix_length": 0,
                          "max_expansions": 50,
                          "lenient": false,
                          "zero_terms_query": "NONE",
                          "boost": 1.0
                      }
                  }
              ],
              $queryDefaults
          }
      }
      """)

      actual should be(expected)
    }

    "return the expected query when an advanced query is given" in {
      val actual = JsonReader.fromString(
        DocumentQuery().chooseMatchQuery(
          AdvancedQuery(queryString), ScoringParamSet(), None).toString)

      val expected = JsonReader.fromString(s"""
      {
          "bool": {
              "should": [
                  {
                      "query_string": {
                          "query": "snuffy",
                          "fields": [
                              "fts_analyzed^1.0",
                              "fts_raw^1.0"
                          ],
                          "use_dis_max": true,
                          "tie_breaker": 0.0,
                          "default_operator": "or",
                          "auto_generate_phrase_queries": true,
                          "max_determined_states": 10000,
                          "enable_position_increment": true,
                          "fuzziness": "AUTO",
                          "fuzzy_prefix_length": 0,
                          "fuzzy_max_expansions": 50,
                          "phrase_slop": 0,
                          "escape": false,
                          "split_on_whitespace": true,
                          "boost": 1.0
                      }
                  },
                  {
                      "has_parent": {
                          "query": {
                              "query_string": {
                                  "query": "snuffy",
                                  "fields": [
                                      "cnames^1.0",
                                      "fts_analyzed^1.0",
                                      "fts_raw^1.0"
                                  ],
                                  "use_dis_max": true,
                                  "tie_breaker": 0.0,
                                  "default_operator": "or",
                                  "auto_generate_phrase_queries": true,
                                  "max_determined_states": 10000,
                                  "enable_position_increment": true,
                                  "fuzziness": "AUTO",
                                  "fuzzy_prefix_length": 0,
                                  "fuzzy_max_expansions": 50,
                                  "phrase_slop": 0,
                                  "escape": false,
                                  "split_on_whitespace": true,
                                  "boost": 1.0
                              }
                          },
                          "parent_type": "domain",
                          "score": false,
                          "ignore_unmapped": false,
                          "boost": 1.0
                      }
                  }
              ],
              $queryDefaults
          }
      }
      """)

      actual should be(expected)
    }
  }

  "DocumentQuery(): categoriesQuery" should {
    "return no query if no categories are given" in {
      DocumentQuery().categoriesQuery(None) should be(None)
    }

    "return the expected query if one category is given" in {
      val actual = JsonReader.fromString(
        DocumentQuery().categoriesQuery(Some(Set("Fun"))).get.toString)

      val expected = JsonReader.fromString(s"""
      {
          "nested": {
              "query": {
                  "bool": {
                      "should": [
                          {
                              "match_phrase": {
                                  "animl_annotations.categories.name.lowercase": {
                                      "query": "Fun",
                                      "slop": 0,
                                      "boost": 1.0
                                  }
                              }
                          }
                      ],
                      "minimum_should_match": "1",
                      $queryDefaults
                  }
              },
              "path": "animl_annotations.categories",
              "ignore_unmapped": false,
              "score_mode": "avg",
              "boost": 1.0
          }
      }
      """)

      actual should be(expected)
    }

    "return the expected query if multiple categories are given" in {
      val actual = JsonReader.fromString(
        DocumentQuery().categoriesQuery(Some(Set("Fun", "Times"))).get.toString)

      val expected = JsonReader.fromString(s"""
      {
          "nested": {
              "query": {
                  "bool": {
                      "should": [
                          {
                              "match_phrase": {
                                  "animl_annotations.categories.name.lowercase": {
                                      "query": "Fun",
                                      "slop": 0,
                                      "boost": 1.0
                                  }
                              }
                          },
                          {
                              "match_phrase": {
                                  "animl_annotations.categories.name.lowercase": {
                                      "query": "Times",
                                      "slop": 0,
                                      "boost": 1.0
                                  }
                              }
                          }
                      ],
                      "minimum_should_match": "1",
                      $queryDefaults
                  }
              },
              "path": "animl_annotations.categories",
              "ignore_unmapped": false,
              "score_mode": "avg",
              "boost": 1.0
          }
      }
      """)

      actual should be(expected)
    }

    "return the expected query if one category with multiple terms is given" in {
      val actual = JsonReader.fromString(
        DocumentQuery().categoriesQuery(Some(Set("Unemployment Insurance"))).get.toString)

      val expected = JsonReader.fromString(s"""
      {
          "nested": {
              "query": {
                  "bool": {
                      "should": [
                          {
                              "match_phrase": {
                                  "animl_annotations.categories.name.lowercase": {
                                      "query": "Unemployment Insurance",
                                      "slop": 0,
                                      "boost": 1.0
                                  }
                              }
                          }
                      ],
                      "minimum_should_match": "1",
                      $queryDefaults
                  }
              },
              "path": "animl_annotations.categories",
              "ignore_unmapped": false,
              "score_mode": "avg",
              "boost": 1.0
          }
      }
      """)

      actual should be(expected)
    }
  }

  "DocumentQuery(): tagsQuery" should {
    "return no query if no tags are given" in {
      DocumentQuery().tagsQuery(None) should be(None)
    }

    "return the expected query if one tag is given" in {
      val actual = JsonReader.fromString(
        DocumentQuery().tagsQuery(Some(Set("Fun"))).get.toString)

      val expected = JsonReader.fromString(s"""
      {
          "nested": {
              "query": {
                  "bool": {
                      "should": [
                          {
                              "match_phrase": {
                                  "animl_annotations.tags.name.lowercase": {
                                      "query": "Fun",
                                      "slop": 0,
                                      "boost": 1.0
                                  }
                              }
                          }
                      ],
                      "minimum_should_match": "1",
                      $queryDefaults
                  }
              },
              "path": "animl_annotations.tags",
              "ignore_unmapped": false,
              "score_mode": "avg",
              "boost": 1.0
          }
      }
      """)

      actual should be(expected)
    }

    "return the expected query if multiple tags are given" in {
      val actual = JsonReader.fromString(
        DocumentQuery().tagsQuery(Some(Set("Fun", "Times"))).get.toString)

      val expected = JsonReader.fromString(s"""
      {
          "nested": {
              "query": {
                  "bool": {
                      "should": [
                          {
                              "match_phrase": {
                                  "animl_annotations.tags.name.lowercase": {
                                      "query": "Fun",
                                      "slop": 0,
                                      "boost": 1.0
                                  }
                              }
                          },
                          {
                              "match_phrase": {
                                  "animl_annotations.tags.name.lowercase": {
                                      "query": "Times",
                                      "slop": 0,
                                      "boost": 1.0
                                  }
                              }
                          }
                      ],
                      "minimum_should_match": "1",
                      $queryDefaults
                  }
              },
              "path": "animl_annotations.tags",
              "ignore_unmapped": false,
              "score_mode": "avg",
              "boost": 1.0
          }
      }
      """)

      actual should be(expected)
    }
  }

  "DocumentQuery(): domainCategoriesQuery" should {
    "return no query if no categories are given" in {
      DocumentQuery().domainCategoriesQuery(None) should be(None)
    }

    "return the expected query if one category is given" in {
      val actual = JsonReader.fromString(
        DocumentQuery().domainCategoriesQuery(Some(Set("Fun"))).get.toString)

      val expected = JsonReader.fromString(s"""
      {
          "bool": {
              "should": [
                  {
                      "match_phrase": {
                          "customer_category.lowercase": {
                              "query": "Fun",
                              "slop": 0,
                              "boost": 1.0
                          }
                      }
                  }
              ],
              "minimum_should_match": "1",
              $queryDefaults
          }
      }
      """)

      actual should be(expected)
    }

    "return the expected query if multiple categories are given" in {
      val actual = JsonReader.fromString(
        DocumentQuery().domainCategoriesQuery(Some(Set("Fun", "Times"))).get.toString)

      val expected = JsonReader.fromString(s"""
      {
          "bool": {
              "should": [
                  {
                      "match_phrase": {
                          "customer_category.lowercase": {
                              "query": "Fun",
                              "slop": 0,
                              "boost": 1.0
                          }
                      }
                  },
                  {
                      "match_phrase": {
                          "customer_category.lowercase": {
                              "query": "Times",
                              "slop": 0,
                              "boost": 1.0
                          }
                      }
                  }
              ],
              "minimum_should_match": "1",
              "disable_coord": false,
              "adjust_pure_negative": true,
              "boost": 1.0
          }
      }
      """)

      actual should be(expected)
    }
  }

  "DocumentQuery(): domainTagsQuery" should {
    "return no query if no tags are given" in {
      DocumentQuery().domainTagsQuery(None) should be(None)
    }

    "return the expected query if one tag is given" in {
      val actual = JsonReader.fromString(
        DocumentQuery().domainTagsQuery(Some(Set("Fun"))).get.toString)

      val expected = JsonReader.fromString(s"""
      {
          "bool": {
              "should": [
                  {
                      "match_phrase": {
                          "customer_tags.lowercase": {
                              "query": "Fun",
                              "slop": 0,
                              "boost": 1.0
                          }
                      }
                  }
              ],
              "minimum_should_match": "1",
              $queryDefaults
          }
      }
      """)

      actual should be(expected)
    }

    "return the expected query if multiple tags are given" in {
      val actual = JsonReader.fromString(
        DocumentQuery().domainTagsQuery(Some(Set("Fun", "Times"))).get.toString)

      val expected = JsonReader.fromString(s"""
      {
          "bool": {
              "should": [
                  {
                      "match_phrase": {
                          "customer_tags.lowercase": {
                              "query": "Fun",
                              "slop": 0,
                              "boost": 1.0
                          }
                      }
                  },
                  {
                      "match_phrase": {
                          "customer_tags.lowercase": {
                              "query": "Times",
                              "slop": 0,
                              "boost": 1.0
                          }
                      }
                  }
              ],
              "minimum_should_match": "1",
              $queryDefaults
          }
      }
      """)

      actual should be(expected)
    }
  }
}
