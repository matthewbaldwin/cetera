package com.socrata.cetera.search

import com.rojoma.json.v3.codec.JsonEncode
import com.rojoma.json.v3.interpolation._
import com.rojoma.json.v3.io.JsonReader
import org.elasticsearch.index.query.FilterBuilder
import org.scalatest.{ShouldMatchers, WordSpec}

import com.socrata.cetera.TestESDomains
import com.socrata.cetera.auth.User
import com.socrata.cetera.errors.UnauthorizedError
import com.socrata.cetera.handlers.SearchParamSet
import com.socrata.cetera.types.{ApprovalStatus, DomainSet, SimpleQuery}

class DocumentFiltersSpec extends WordSpec with ShouldMatchers with TestESDomains {

  val docFilters = DocumentFilters(forDomainSearch = false)
  val docFiltersForDomainSearch = DocumentFilters(forDomainSearch = true)

  def behaveLikeABooleanTermFilter(filter: Boolean => FilterBuilder, termKey: String, forDomainSearch: Boolean): Unit = {
    val key = if (forDomainSearch) "document." + termKey else termKey
    s"return the expected boolean term filter with key $key" in {
      val trueParamFilter = filter(true)
      val falseParamFilter = filter(false)
      val key = if (forDomainSearch) s"document.$termKey" else termKey
      val expectedTrueFilter = j"""{"term" :{$key : true}}"""
      val expectedFalseFilter = j"""{"term" :{$key : false}}"""
      val actualTrueFilter = JsonReader.fromString(trueParamFilter.toString)
      val actuaFalseFilter = JsonReader.fromString(falseParamFilter.toString)
      actualTrueFilter should be(expectedTrueFilter)
      actuaFalseFilter should be(expectedFalseFilter)
    }
  }

  def behaveLikeATermFilter[T: JsonEncode](filter: T => FilterBuilder, termKey: String, termValue: T, forDomainSearch: Boolean): Unit = {
    val key = if (forDomainSearch) "document." + termKey else termKey
    s"return the expected term filter with key $key and value $termValue" in {
      val expected = j"""{ "term": { $key: $termValue } }"""
      val actual = JsonReader.fromString(filter(termValue).toString)
      actual should be(expected)
    }
  }

  def behaveLikeAStatusTermFilter(filter: ApprovalStatus => FilterBuilder, termKey: String, termValue: ApprovalStatus, forDomainSearch: Boolean): Unit = {
    val key = if (forDomainSearch) "document." + termKey else termKey
    s"return the expected term filter with key $key and value ${termValue.status}" in {
      val expected = j"""{ "term": { $key: ${termValue.status} } }"""
      val actual = JsonReader.fromString(filter(termValue).toString)
      actual should be(expected)
    }
  }

  def behaveLikeATermsFilter[T: JsonEncode](filter: Set[T] => FilterBuilder, termKey: String, termValues: Set[T], forDomainSearch: Boolean): Unit = {
    val key = if (forDomainSearch) "document." + termKey else termKey
    s"return the expected terms filter with key $key and values $termValues" in {
      termValues.size should be > 1 // to test empty sets and sets of 1 and sets of many
      val singleTermValue = termValues.head
      val emptyFilter = filter(Set.empty[T])
      val singleFilter = filter(Set(singleTermValue))
      val multipleFilter = filter(termValues)

      val expectedEmptyFilter = j"""{ "terms": { $key: [ ] } }"""
      val expectedSingleFilter = j"""{ "terms": { $key: [ $singleTermValue ] } }"""
      val expectedMultipleFilter = j"""{ "terms": { $key: [ ${termValues.mkString(",")} ] } }"""

      val actualEmptyFilter = JsonReader.fromString(expectedEmptyFilter.toString)
      val actualSingleFilter = JsonReader.fromString(expectedSingleFilter.toString)
      val actualMultipleFilter = JsonReader.fromString(expectedMultipleFilter.toString)

      actualEmptyFilter should be(expectedEmptyFilter)
      actualSingleFilter should be(expectedSingleFilter)
      actualMultipleFilter should be(expectedMultipleFilter)
    }
  }

  "the datatypeFilter" should {
    behaveLikeATermsFilter(docFilters.datatypeFilter, "datatype", Set("datasets", "links"), forDomainSearch = false)
    behaveLikeATermsFilter(docFiltersForDomainSearch.datatypeFilter, "datatype", Set("datasets", "links"), forDomainSearch = true)
  }

  "the userFilter" should {
    behaveLikeATermFilter(docFilters.userFilter, "owner_id", "wonder-woman", forDomainSearch = false)
    behaveLikeATermFilter(docFiltersForDomainSearch.userFilter, "owner_id", "wonder-woman", forDomainSearch = true)
  }

  "the sharedToFilter" should {
    behaveLikeATermFilter(docFilters.sharedToFilter, "shared_to", "wonder-woman", forDomainSearch = false)
    behaveLikeATermFilter(docFiltersForDomainSearch.sharedToFilter, "shared_to", "wonder-woman", forDomainSearch = true)
  }

  "the attributionFilter" should {
    behaveLikeATermFilter(docFilters.attributionFilter, "attribution.raw", "wonder-woman", forDomainSearch = false)
    behaveLikeATermFilter(docFiltersForDomainSearch.attributionFilter, "attribution.raw", "wonder-woman", forDomainSearch = true)
  }

  "the provenanceFilter" should {
    behaveLikeATermFilter(docFilters.provenanceFilter, "provenance", "official", forDomainSearch = false)
    behaveLikeATermFilter(docFiltersForDomainSearch.provenanceFilter, "provenance", "official", forDomainSearch = true)
  }

  "the licenseFilter" should {
    behaveLikeATermFilter(docFilters.licenseFilter, "license", "WTFPL", forDomainSearch = false)
    behaveLikeATermFilter(docFiltersForDomainSearch.licenseFilter, "license", "WTFPL", forDomainSearch = true)
  }

  "the parentDatasetFilter" should {
    behaveLikeATermFilter(docFilters.parentDatasetFilter, "socrata_id.parent_dataset_id", "four-four", forDomainSearch = false)
    behaveLikeATermFilter(docFiltersForDomainSearch.parentDatasetFilter, "socrata_id.parent_dataset_id", "four-four", forDomainSearch = true)
  }

  "the hiddenFromCatalogFilter" should {
    behaveLikeABooleanTermFilter(docFilters.hiddenFromCatalogFilter, "hide_from_catalog", forDomainSearch = false)
    behaveLikeABooleanTermFilter(docFiltersForDomainSearch.hiddenFromCatalogFilter, "hide_from_catalog", forDomainSearch = true)
  }

  "the idFilter" should {
    behaveLikeATermsFilter(docFilters.idFilter, "_id", Set("enye-3kdf", "m9se-we56"), forDomainSearch = false)
    behaveLikeATermsFilter(docFiltersForDomainSearch.idFilter, "_id", Set("enye-3kdf", "m9se-we56"), forDomainSearch = true)
  }

  "the domainIdFilter" should {
    behaveLikeATermsFilter(docFilters.domainIdFilter, "socrata_id.domain_id", Set(0, 1), forDomainSearch = false)
    behaveLikeATermsFilter(docFiltersForDomainSearch.domainIdFilter, "socrata_id.domain_id", Set(0, 1), forDomainSearch = true)
  }

  "the approved raStatusAccordingToParentDomainFilter" should {
    behaveLikeABooleanTermFilter(docFilters.raStatusAccordingToParentDomainFilter(ApprovalStatus.approved, _), "is_approved_by_parent_domain", forDomainSearch = false)
    behaveLikeABooleanTermFilter(docFiltersForDomainSearch.raStatusAccordingToParentDomainFilter(ApprovalStatus.approved, _), "is_approved_by_parent_domain", forDomainSearch = true)
  }

  "the pending raStatusAccordingToParentDomainFilter" should {
    behaveLikeABooleanTermFilter(docFilters.raStatusAccordingToParentDomainFilter(ApprovalStatus.pending, _), "is_pending_on_parent_domain", forDomainSearch = false)
    behaveLikeABooleanTermFilter(docFiltersForDomainSearch.raStatusAccordingToParentDomainFilter(ApprovalStatus.pending, _), "is_pending_on_parent_domain", forDomainSearch = true)
  }

  "the rejected raStatusAccordingToParentDomainFilter" should {
    behaveLikeABooleanTermFilter(docFilters.raStatusAccordingToParentDomainFilter(ApprovalStatus.rejected, _), "is_rejected_by_parent_domain", forDomainSearch = false)
    behaveLikeABooleanTermFilter(docFiltersForDomainSearch.raStatusAccordingToParentDomainFilter(ApprovalStatus.rejected, _), "is_rejected_by_parent_domain", forDomainSearch = true)
  }

  "the approved raStatusAccordingToContextFilter" should {
    behaveLikeATermFilter[Int](docFilters.raStatusAccordingToContextFilter(ApprovalStatus.approved, _), "approving_domain_ids", 3, forDomainSearch = false)
    behaveLikeATermFilter[Int](docFiltersForDomainSearch.raStatusAccordingToContextFilter(ApprovalStatus.approved, _), "approving_domain_ids", 3, forDomainSearch = true)
  }

  "the pending raStatusAccordingToContextFilter" should {
    behaveLikeATermFilter[Int](docFilters.raStatusAccordingToContextFilter(ApprovalStatus.pending, _), "pending_domain_ids", 3, forDomainSearch = false)
    behaveLikeATermFilter[Int](docFiltersForDomainSearch.raStatusAccordingToContextFilter(ApprovalStatus.pending, _), "pending_domain_ids", 3, forDomainSearch = true)
  }

  "the rejected raStatusAccordingToContextFilter" should {
    behaveLikeATermFilter[Int](docFilters.raStatusAccordingToContextFilter(ApprovalStatus.rejected, _), "rejecting_domain_ids", 3, forDomainSearch = false)
    behaveLikeATermFilter[Int](docFiltersForDomainSearch.raStatusAccordingToContextFilter(ApprovalStatus.rejected, _), "rejecting_domain_ids", 3, forDomainSearch = true)
  }

  "the approved modStatusFilter" should {
    behaveLikeAStatusTermFilter(docFilters.modStatusFilter, "moderation_status", ApprovalStatus.approved, forDomainSearch = false)
    behaveLikeAStatusTermFilter(docFiltersForDomainSearch.modStatusFilter, "moderation_status", ApprovalStatus.approved, forDomainSearch = true)
  }

  "the pending modStatusFilter" should {
    behaveLikeAStatusTermFilter(docFilters.modStatusFilter, "moderation_status", ApprovalStatus.pending, forDomainSearch = false)
    behaveLikeAStatusTermFilter(docFiltersForDomainSearch.modStatusFilter, "moderation_status", ApprovalStatus.pending, forDomainSearch = true)
  }

  "the rejected modStatusFilter" should {
    behaveLikeAStatusTermFilter(docFilters.modStatusFilter, "moderation_status", ApprovalStatus.rejected, forDomainSearch = false)
    behaveLikeAStatusTermFilter(docFiltersForDomainSearch.modStatusFilter, "moderation_status", ApprovalStatus.rejected, forDomainSearch = true)
  }

  "the defaultViewFilter" should {
    behaveLikeABooleanTermFilter(docFilters.defaultViewFilter, "is_default_view", forDomainSearch = false)
    behaveLikeABooleanTermFilter(docFiltersForDomainSearch.defaultViewFilter, "is_default_view", forDomainSearch = true)
  }

  "the publicFilter" should {
    behaveLikeABooleanTermFilter(docFilters.publicFilter, "is_public", forDomainSearch = false)
    behaveLikeABooleanTermFilter(docFiltersForDomainSearch.publicFilter, "is_public", forDomainSearch = true)
  }

  "the publishedFilter" should {
    behaveLikeABooleanTermFilter(docFilters.publishedFilter, "is_published", forDomainSearch = false)
    behaveLikeABooleanTermFilter(docFiltersForDomainSearch.publishedFilter, "is_published", forDomainSearch = true)
  }

  "the customerCategoriesFilter" should {
    behaveLikeATermsFilter(docFilters.customerCategoriesFilter, "customer_categories", Set("cats", "dogs"), forDomainSearch = false)
    behaveLikeATermsFilter(docFiltersForDomainSearch.customerCategoriesFilter, "customer_categories", Set("cats", "dogs"), forDomainSearch = true)
  }

  "the customerTagsFilter" should {
    behaveLikeATermsFilter(docFilters.customerTagsFilter, "customer_tags", Set("pumas", "cheetahs"), forDomainSearch = false)
    behaveLikeATermsFilter(docFiltersForDomainSearch.customerTagsFilter, "customer_tags", Set("pumas", "cheetahs"), forDomainSearch = true)
  }

  "the privateMetadataUserRestrictionsFilter" should {
    "return the expected filter when the user doesn't have a blessed role" in {
      val user = User("user-fxf")
      val filter = docFilters.privateMetadataUserRestrictionsFilter(user)
      val expected = j"""{ "bool":{ "should" :
              [ { "term" : { "owner_id" : "user-fxf" }},
                { "term" : { "shared_to" : "user-fxf" }}]}}"""
      val actual = JsonReader.fromString(filter.toString)
      actual should be(expected)
    }

    "return the expected filter when the user does have a blessed role" in {
      val user = User("user-fxf", Some(domains(0)), Some("publisher"))
      val filter = docFilters.privateMetadataUserRestrictionsFilter(user)
      val expected = j"""{ "bool":{ "should" :
              [ { "term" : { "owner_id" : "user-fxf" }},
                { "term" : { "shared_to" : "user-fxf" }},
                { "terms": { "socrata_id.domain_id": [ 0 ]}}]}}"""
      val actual = JsonReader.fromString(filter.toString)
      actual should be(expected)
    }
  }

  "the domainMetadataFilter when public is true" should {
    "return the expected filter when an empty set of metadata is given" in {
      val filter = docFilters.metadataFilter(Set.empty, public = true)
      filter should be(None)
    }

    "return the expected filter when a metadata query lists a single key, single value pair" in {
      val filter = docFilters.metadataFilter(Set(("org", "ny")), public = true)
      val expected =
        j"""{"bool": {"should": {"nested": {"filter": {"bool": {"must": [
            {"terms": {"customer_metadata_flattened.key.raw": [ "org" ] }},
            {"terms": {"customer_metadata_flattened.value.raw": [ "ny" ] }}
          ]}}, "path": "customer_metadata_flattened" }}}
      }"""
      val actual = JsonReader.fromString(filter.get.toString)
      actual should be(expected)
    }

    "return the expected filter when a metadata query lists a single key but multiple values" in {
      val filter = docFilters.metadataFilter(Set(("org", "ny"), ("org", "nj")), public = true)
      val expected =
        j"""{"bool": {"should": [
            {"nested": {"filter": {"bool": {"must": [
              {"terms": {"customer_metadata_flattened.key.raw": [ "org" ] }},
              {"terms": {"customer_metadata_flattened.value.raw": [ "ny" ] }}
            ]}}, "path": "customer_metadata_flattened" }},
            {"nested": {"filter": {"bool": {"must": [
              {"terms": {"customer_metadata_flattened.key.raw": [ "org" ] }},
              {"terms": {"customer_metadata_flattened.value.raw": [ "nj" ] }}
            ]}}, "path": "customer_metadata_flattened" }}]}
      }"""
      val actual = JsonReader.fromString(filter.get.toString)
      actual should be(expected)
    }

    "return the expected filter when a metadata query lists multiple sets of single key/single value pairs" in {
      val filter = docFilters.metadataFilter(Set(("org", "chicago"), ("owner", "john")), public = true)
      val expected =
        j"""{"bool": {"must": [
               {"bool": {"should": {"nested": {"filter": {"bool": {"must": [
                 {"terms": {"customer_metadata_flattened.key.raw": [ "org" ] }},
                 {"terms": {"customer_metadata_flattened.value.raw": [ "chicago" ] }}
               ]}}, "path": "customer_metadata_flattened" }}}},
               {"bool": {"should": {"nested": {"filter": {"bool": {"must": [
                 {"terms": {"customer_metadata_flattened.key.raw": [ "owner" ] }},
                 {"terms": {"customer_metadata_flattened.value.raw": [ "john" ] }}
               ]}}, "path": "customer_metadata_flattened" }}}}
             ]}
      }"""
      val actual = JsonReader.fromString(filter.get.toString)
      actual should be(expected)
    }

    "return the expected filter when a metadata query lists multiple sets of keys with multiple values" in {
      val filter = docFilters.metadataFilter(Set(("org", "chicago"), ("owner", "john"), ("org", "ny")), public = true)
      val expected =
        j"""{"bool": {"must": [
               {"bool": {"should": [
                  {"nested": {"filter": {"bool": {"must": [
                    {"terms": {"customer_metadata_flattened.key.raw": [ "org" ] }},
                    {"terms": {"customer_metadata_flattened.value.raw": [ "chicago" ] }}
                  ]}}, "path": "customer_metadata_flattened" }},
                  {"nested": {"filter": {"bool": {"must": [
                    {"terms": {"customer_metadata_flattened.key.raw": [ "org" ] }},
                    {"terms": {"customer_metadata_flattened.value.raw": [ "ny" ] }}
                  ]}}, "path": "customer_metadata_flattened" }}]}
               },
               {"bool": {"should": {"nested": {"filter": {"bool": {"must": [
                 {"terms": {"customer_metadata_flattened.key.raw": [ "owner" ] }},
                 {"terms": {"customer_metadata_flattened.value.raw": [ "john" ] }}
               ]}}, "path": "customer_metadata_flattened" }}}}
             ]}
        }"""
      val actual = JsonReader.fromString(filter.get.toString)
      actual should be(expected)
    }
  }

  "the privateDomainMetadataFilter" should {
    "return None if no user is given" in {
      val filter = docFilters.privateMetadataFilter(Set.empty, None)
      filter should be(None)
    }

    "return None if the user doesn't have an authenticating domain" in {
      val user = User("mooks")
      val filter = docFilters.privateMetadataFilter(Set.empty, Some(user))
      filter should be(None)
    }

    "return a basic metadata filter when the user is a super admin" in {
      val user = User("mooks", flags = Some(List("admin")))
      val metadata = Set(("org", "ny"))
      val actual = JsonReader.fromString(docFilters.privateMetadataFilter(metadata, Some(user)).get.toString)
      val expected = JsonReader.fromString(docFilters.metadataFilter(metadata, public = false).get.toString)
      actual should be(expected)
    }

    "return the expected filter when the user is authenticated" in {
      val user = User("mooks", Some(domains(0)), Some("publisher"))
      val metadata = Set(("org", "ny"), ("org", "nj"))
      val filter = docFilters.privateMetadataFilter(metadata, Some(user))
      val metaFilter = JsonReader.fromString(docFilters.metadataFilter(metadata, public = false).get.toString)
      val privacyFilter = JsonReader.fromString(docFilters.privateMetadataUserRestrictionsFilter(user).toString)
      val expected = j"""{"bool": {"must": [ $privacyFilter, $metaFilter ]}}"""
      val actual = JsonReader.fromString(filter.get.toString)
      actual should be(expected)
    }
  }

  "the combinedMetadataFilter" should {
    "return None if neither public or private metadata filters are created" in {
      val user = User("mooks", flags = Some(List("admin")))
      val filter = docFilters.combinedMetadataFilter(Set.empty, Some(user))
      filter should be(None)
    }

    "return only a public metadata filter if the user isn't authenticated" in {
      val user = User("mooks")
      val metadata = Set(("org", "ny"))
      val actual = JsonReader.fromString(docFilters.combinedMetadataFilter(metadata, Some(user)).get.toString)
      val expected = JsonReader.fromString(docFilters.metadataFilter(metadata, public = true).get.toString)
      actual should be(expected)
    }

    "return both public and private metadata filters if the user is authenticated" in {
      val user = User("mooks", Some(domains(0)), Some("publisher"))
      val metadata = Set(("org", "ny"), ("org", "nj"))
      val filter = docFilters.combinedMetadataFilter(metadata, Some(user))
      val pubFilter = JsonReader.fromString(docFilters.metadataFilter(metadata, public = true).get.toString)
      val privateFilter = JsonReader.fromString(docFilters.privateMetadataFilter(metadata, Some(user)).get.toString)
      val expected = j"""{"bool": {"should": [ $pubFilter, $privateFilter ]}}"""
      val actual = JsonReader.fromString(filter.get.toString)
      actual should be(expected)
    }
  }

  "the vmSearchContextFilter" should {
    "return None if there is no search context" in {
      val domainSet = DomainSet(Set(domains(0)), None)
      val filter = docFilters.vmSearchContextFilter(domainSet)
      filter should be(None)
    }

    "return None if the search context isn't moderated" in {
      val domainSet = DomainSet(Set(domains(0)), Some(domains(0)))
      val filter = docFilters.vmSearchContextFilter(domainSet)
      filter should be(None)
    }

    "return the expected filter if the search context is moderated" in {
      val domainSet = DomainSet(Set(domains(1), domains(0)), Some(domains(1)))
      val defaultFilter = j"""{ "term": { "is_default_view": true } }"""
      val fromModeratedDomain = j"""{ "terms": { "socrata_id.domain_id": [ 1 ] } }"""
      val expected = j"""{"bool": {"should": [$defaultFilter, $fromModeratedDomain]}}"""
      val filter = docFilters.vmSearchContextFilter(domainSet).get
      val actual = JsonReader.fromString(filter.toString)
      actual should be(expected)
    }
  }

  "the raSearchContextFilter" should {
    "return None if there is no search context" in {
      val domainSet = DomainSet(Set(domains(2)), None)
      val filter = docFilters.raSearchContextFilter(domainSet)
      filter should be(None)
    }

    "return None if the search context doesn't have R&A enabled" in {
      val domainSet = DomainSet(Set(domains(1)), Some(domains(1)))
      val filter = docFilters.raSearchContextFilter(domainSet)
      filter should be(None)
    }

    "return the expected filter if the search context does have R&A enabled" in {
      val domainSet = DomainSet(Set(domains(2), domains(3)), Some(domains(3)))
      val approved = j"""{ "term": { "approving_domain_ids": 3 } }"""
      val rejected = j"""{ "term": { "rejecting_domain_ids": 3 } }"""
      val pending = j"""{ "term": { "pending_domain_ids": 3 } }"""
      val expected = j"""{"bool" : {"should" : [$approved, $rejected, $pending]}}"""
      val filter = docFilters.raSearchContextFilter(domainSet).get
      val actual = JsonReader.fromString(filter.toString)
      actual should be(expected)
    }
  }

  "the searchContextFilter" should {
    "return None if there is no search context" in {
      val domainSet = DomainSet(Set(domains(2)), None)
      val filter = docFilters.searchContextFilter(domainSet)
      filter should be(None)
    }

    "return None if the search context has neither R&A or VM enabled" in {
      val domainSet = DomainSet(Set(domains(1)), Some(domains(0)))
      val filter = docFilters.searchContextFilter(domainSet)
      filter should be(None)
    }

    "return the expected filter if the search context has both R&A and VM enabled" in {
      val domainSet = DomainSet(Set(domains(2), domains(3)), Some(domains(3)))
      val vmFilter = JsonReader.fromString(docFilters.vmSearchContextFilter(domainSet).get.toString())
      val raFilter = JsonReader.fromString(docFilters.raSearchContextFilter(domainSet).get.toString())
      val expected = j"""{"bool" : {"must" : [$vmFilter, $raFilter]}}"""
      val filter = docFilters.searchContextFilter(domainSet).get
      val actual = JsonReader.fromString(filter.toString)
      actual should be(expected)
    }
  }

  "the domainSetFilter" should {
    "return a basic domain filter if there is no search context" in {
      val domainSet = DomainSet(Set(domains(2)), None)
      val domainFilter = JsonReader.fromString(docFilters.domainIdFilter(Set(2)).toString)
      val expected = j"""{"bool": {"must": $domainFilter}}"""
      val filter = docFilters.domainSetFilter(domainSet)
      val actual = JsonReader.fromString(filter.toString)
      actual should be(expected)
    }

    "return a basic domain filter if the search context has neither R&A or VM enabled" in {
      val domainSet = DomainSet(Set(domains(1)), Some(domains(0)))
      val domainFilter = JsonReader.fromString(docFilters.domainIdFilter(Set(1)).toString)
      val expected = j"""{"bool": {"must": $domainFilter}}"""
      val filter = docFilters.domainSetFilter(domainSet)
      val actual = JsonReader.fromString(filter.toString)
      actual should be(expected)
    }

    "return the expected filter if the search context has both R&A and VM enabled" in {
      val domainSet = DomainSet(Set(domains(2), domains(3)), Some(domains(2)))
      val domainFilter = JsonReader.fromString(docFilters.domainIdFilter(Set(2, 3)).toString)
      val contextFilter = JsonReader.fromString(docFilters.searchContextFilter(domainSet).get.toString)
      val expected = j"""{"bool": {"must": [$domainFilter, $contextFilter]}}"""
      val filter = docFilters.domainSetFilter(domainSet)
      val actual = JsonReader.fromString(filter.toString)
      actual should be(expected)
    }
  }

  "the moderationStatusFilter for approved" should {
    val defaultFilter = j"""{ "term": { "is_default_view": true } }"""
    val approvedFilter = j"""{ "term": { "moderation_status": "approved" } }"""

    "include only default/approved views if there are no unmoderated domains" in {
      val domainSet = DomainSet(Set(domains(1), domains(3)))
      val filter = docFilters.moderationStatusFilter(ApprovalStatus.approved, domainSet)
      val fromNoDomain = j"""{ "terms" : { "socrata_id.domain_id" : [] } }"""
      val expected = j"""{"bool": {"should": [$defaultFilter, $approvedFilter, $fromNoDomain]}}"""
      val actual = JsonReader.fromString(filter.toString)
      actual should be(expected)
    }

    "include default/approved views + those from unmoderated domain if there are some unmoderated domains" in {
      val domainSet = DomainSet(Set(domains(0), domains(2), domains(3)))
      val filter = docFilters.moderationStatusFilter(ApprovalStatus.approved, domainSet)
      val fromUnmoderatedDomain = j"""{ "terms": { "socrata_id.domain_id": [ 0, 2 ] } }"""
      val expected = j"""{"bool": {"should": [$defaultFilter, $approvedFilter, $fromUnmoderatedDomain]}}"""
      val actual = JsonReader.fromString(filter.toString)
      actual should be(expected)
    }
  }

  "the moderationStatusFilter for rejected" should {
    val rejectedFilter = j"""{ "term": { "moderation_status": "rejected" } }"""
    val beDerived = j"""{ "term" : { "is_default_view" : false } }"""

    "return the expected document-eliminating filter if there are no moderated domains" in {
      val domainSet = DomainSet(Set(domains(0), domains(2)))
      val fromNoDomain = j"""{ "terms" : { "socrata_id.domain_id" : [] } }"""
      val expected = j"""{"bool": {"must": [$fromNoDomain, $beDerived, $rejectedFilter]}}"""
      val filter = docFilters.moderationStatusFilter(ApprovalStatus.rejected, domainSet)
      val actual = JsonReader.fromString(filter.toString)
      actual should be(expected)
    }

    "return rejected views from moderated domain if there are some moderated domains" in {
      val domainSet = DomainSet(Set(domains(1), domains(2), domains(3)))
      val filter = docFilters.moderationStatusFilter(ApprovalStatus.rejected, domainSet)
      val fromModeratedDomain = j"""{ "terms": { "socrata_id.domain_id": [ 1, 3 ] } }"""
      val expected = j"""{"bool": {"must": [$fromModeratedDomain, $beDerived, $rejectedFilter]}}"""
      val actual = JsonReader.fromString(filter.toString)
      actual should be(expected)
    }
  }

  "the moderationStatusFilter for pending" should {
    val pendingFilter = j"""{ "term": { "moderation_status": "pending" } }"""
    val beDerived = j"""{ "term" : { "is_default_view" : false } }"""

    "return the expected document-eliminating filter if there are no moderated domains" in {
      val domainSet = DomainSet(Set(domains(0), domains(2)))
      val fromNoDomain = j"""{ "terms" : { "socrata_id.domain_id" : [] } }"""
      val expected = j"""{"bool": {"must": [$fromNoDomain, $beDerived, $pendingFilter]}}"""
      val filter = docFilters.moderationStatusFilter(ApprovalStatus.pending, domainSet)
      val actual = JsonReader.fromString(filter.toString)
      actual should be(expected)
    }

    "return rejected views from moderated domain if there are some moderated domains" in {
      val domainSet = DomainSet(Set(domains(1), domains(2), domains(3)))
      val filter = docFilters.moderationStatusFilter(ApprovalStatus.pending, domainSet)
      val fromModeratedDomain = j"""{ "terms": { "socrata_id.domain_id": [ 1, 3 ] } }"""
      val expected = j"""{"bool": {"must": [$fromModeratedDomain, $beDerived, $pendingFilter]}}"""
      val actual = JsonReader.fromString(filter.toString)
      actual should be(expected)
    }
  }

  "the datalensStatusFilter" should {
    val datalensFilter = j"""{"terms" :{"datatype" :["datalens","datalens_chart","datalens_map"]}}"""

    "include all views that are not unapproved datalens if looking for approved" in {
      val filter = docFilters.datalensStatusFilter(ApprovalStatus.approved)
      val unApprovedFilter = j"""{"not" :{"filter" :{"term" :{"moderation_status" : "approved"}}}}"""
      val expected = j"""{"not" :{"filter" :{"bool" :{"must" :[$datalensFilter, $unApprovedFilter]}}}}"""
      val actual = JsonReader.fromString(filter.toString)
      actual should be(expected)
    }

    "include only rejected datalens if looking for rejected" in {
      val filter = docFilters.datalensStatusFilter(ApprovalStatus.rejected)
      val rejectedFilter = j"""{ "term": { "moderation_status": "rejected" } }"""
      val expected = j"""{"bool": {"must": [$datalensFilter, $rejectedFilter]}}"""
      val actual = JsonReader.fromString(filter.toString)
      actual should be(expected)
    }

    "include only pending datalens if looking for pending" in {
      val filter = docFilters.datalensStatusFilter(ApprovalStatus.pending)
      val pendingFilter = j"""{ "term": { "moderation_status": "pending" } }"""
      val expected = j"""{"bool": {"must": [$datalensFilter, $pendingFilter]}}"""
      val actual = JsonReader.fromString(filter.toString)
      actual should be(expected)
    }
  }

  "the raStatusFilter" should {
    val rejected = j"""{ "term": { "is_rejected_by_parent_domain": true } }"""
    val pending = j"""{ "term": { "is_pending_on_parent_domain": true } }"""
    val approved =  j"""{ "term": { "is_pending_on_parent_domain": true } }"""
    val fromCustomerDomainsWithRA = j"""{ "terms" : { "socrata_id.domain_id" : [4, 3, 2] } }"""

    "return the expected filter when looking for rejected regardless of context" in {
      val status = ApprovalStatus.rejected
      val domainSetNoContext = DomainSet((0 to 4).map(domains(_)).toSet, None)
      val domainSetRaDisabledContext = domainSetNoContext.copy(searchContext = Some(domains(0)))
      val domainSetRaEnabledContext = domainSetNoContext.copy(searchContext = Some(domains(2)))
      val expected = j"""{"bool": {"must": [$fromCustomerDomainsWithRA, $rejected]}}"""

      val filterNoContext = JsonReader.fromString(docFilters.raStatusFilter(status, domainSetNoContext).toString)
      val filterRaDisabledContext = JsonReader.fromString(docFilters.raStatusFilter(status, domainSetRaDisabledContext).toString)
      val filterRaEnabledContext = JsonReader.fromString(docFilters.raStatusFilter(status, domainSetRaEnabledContext).toString)

      filterNoContext should be(expected)
      filterRaDisabledContext should be(expected)
      filterRaEnabledContext should be(expected)
    }

    "return the expected filter when looking for pending regardless of context" in {
      val status = ApprovalStatus.pending
      val domainSetNoContext = DomainSet((0 to 4).map(domains(_)).toSet, None)
      val domainSetRaDisabledContext = domainSetNoContext.copy(searchContext = Some(domains(0)))
      val domainSetRaEnabledContext = domainSetNoContext.copy(searchContext = Some(domains(2)))
      val expected = j"""{"bool": {"must": [$fromCustomerDomainsWithRA, $pending]}}"""

      val filterNoContext = JsonReader.fromString(docFilters.raStatusFilter(status, domainSetNoContext).toString)
      val filterRaDisabledContext = JsonReader.fromString(docFilters.raStatusFilter(status, domainSetRaDisabledContext).toString)
      val filterRaEnabledContext = JsonReader.fromString(docFilters.raStatusFilter(status, domainSetRaEnabledContext).toString)

      filterNoContext should be(expected)
      filterRaDisabledContext should be(expected)
      filterRaEnabledContext should be(expected)
    }

    "return the expected filter when looking for approved regardless of context" in {
      val status = ApprovalStatus.approved
      val domainSetNoContext = DomainSet((0 to 4).map(domains(_)).toSet, None)
      val domainSetRaDisabledContext = domainSetNoContext.copy(searchContext = Some(domains(0)))
      val domainSetRaEnabledContext = domainSetNoContext.copy(searchContext = Some(domains(2)))
      val approvedByParent = j"""{ "term": { "is_approved_by_parent_domain": true } }"""
      val fromRADisabledDomain = j"""{ "terms": { "socrata_id.domain_id": [ 1, 0 ] } }"""
      val expected = j"""{"bool": {"should": [$fromRADisabledDomain, $approvedByParent]}}"""

      val filterNoContext = JsonReader.fromString(docFilters.raStatusFilter(status, domainSetNoContext).toString)
      val filterRaDisabledContext = JsonReader.fromString(docFilters.raStatusFilter(status, domainSetRaDisabledContext).toString)
      val filterRaEnabledContext = JsonReader.fromString(docFilters.raStatusFilter(status, domainSetRaEnabledContext).toString)

      filterNoContext should be(expected)
      filterRaDisabledContext should be(expected)
      filterRaEnabledContext should be(expected)
    }
  }

  "the raStatusOnContextFilter" should {
    val someDomains = (0 to 4).map(domains(_)).toSet
    val raEnabledContext = Some(domains(2))
    val raDisabledContext = Some(domains(0))

    "return None if there is no context, regardless of status" in {
      val domainSet = DomainSet(someDomains, None)
      val approvedFilter = docFilters.raStatusOnContextFilter(ApprovalStatus.approved, domainSet)
      val rejectedFilter = docFilters.raStatusOnContextFilter(ApprovalStatus.rejected, domainSet)
      val pendingFilter = docFilters.raStatusOnContextFilter(ApprovalStatus.pending, domainSet)

      approvedFilter should be(None)
      rejectedFilter should be(None)
      pendingFilter should be(None)
    }

    "return None if the context doesn't have R&A, regardless of status" in {
      val domainSet = DomainSet(someDomains, raDisabledContext)
      val approvedFilter = docFilters.raStatusOnContextFilter(ApprovalStatus.approved, domainSet)
      val rejectedFilter = docFilters.raStatusOnContextFilter(ApprovalStatus.rejected, domainSet)
      val pendingFilter = docFilters.raStatusOnContextFilter(ApprovalStatus.pending, domainSet)

      approvedFilter should be(None)
      rejectedFilter should be(None)
      pendingFilter should be(None)
    }

    "return the expected filter when looking for approved on a search context with R&A" in {
      val domainSet = DomainSet(someDomains, raEnabledContext)
      val expected = j"""{ "term": { "approving_domain_ids": 2 } }"""
      val filter = docFilters.raStatusOnContextFilter(ApprovalStatus.approved, domainSet)
      val actual = JsonReader.fromString(filter.get.toString)
      actual should be(expected)
    }

    "return the expected filter when looking for rejected on a search context with R&A" in {
      val domainSet = DomainSet(someDomains, raEnabledContext)
      val expected = j"""{ "term": { "rejecting_domain_ids": 2 } }"""
      val filter = docFilters.raStatusOnContextFilter(ApprovalStatus.rejected, domainSet)
      val actual = JsonReader.fromString(filter.get.toString)
      actual should be(expected)
    }

    "return the expected filter when looking for pending on a search context with R&A" in {
      val domainSet = DomainSet(someDomains, raEnabledContext)
      val expected = j"""{ "term": { "pending_domain_ids": 2 } }"""
      val filter = docFilters.raStatusOnContextFilter(ApprovalStatus.pending, domainSet)
      val actual = JsonReader.fromString(filter.get.toString)
      actual should be(expected)
    }
  }

  "the approvalStatusFilter" should {
    val someDomains = (0 to 4).map(domains(_)).toSet
    val defaultFilter = j"""{ "term": { "is_default_view": true } }"""
    val approvedFilter = j"""{ "term": { "moderation_status" : "approved" } }"""
    val defaultOrApprovedFilter = j"""{"bool": {"should": [$defaultFilter, $approvedFilter]}}"""
    val fromModeratedDomain = j"""{ "terms": { "socrata_id.domain_id": [ 1, 3 ] } }"""
    val fromUnmoderatedDomain = j"""{ "terms": { "socrata_id.domain_id": [ 0, 2 ] } }"""
    val fromNoDomain = j"""{ "terms" : { "socrata_id.domain_id" : [] } }"""
    val defaultOrApprovedFilterForUnmoderatedContext =
      j"""{"bool": {"should": [$defaultFilter, $approvedFilter, $fromNoDomain]}}"""
    val moderatedDomains = Set(domains(1), domains(3))
    val unmoderatedDomains = Set(domains(0), domains(2))

    "return the expected filter for approved views if the status is approved and we include the context" in {
      val domainSet = DomainSet(someDomains, Some(domains(3)))
      val beModApproved = JsonReader.fromString(docFilters.moderationStatusFilter(ApprovalStatus.approved, domainSet).toString)
      val beDatalensApproved = JsonReader.fromString(docFilters.datalensStatusFilter(ApprovalStatus.approved).toString)
      val beRAApproved = JsonReader.fromString(docFilters.raStatusFilter(ApprovalStatus.approved, domainSet).toString)
      val beRAApprovedOnContext = JsonReader.fromString(docFilters.raStatusOnContextFilter(ApprovalStatus.approved, domainSet).get.toString)
      val expected = j"""{ "bool": { "must": [$beModApproved, $beDatalensApproved, $beRAApproved, $beRAApprovedOnContext]}}"""

      val filter = docFilters.approvalStatusFilter(ApprovalStatus.approved, domainSet, includeContextApproval = true)
      val actual = JsonReader.fromString(filter.toString)
      actual should be(expected)
    }

    "return the expected filter for approved views if the status is approved and we exclude the context" in {
      val domainSet = DomainSet(someDomains, Some(domains(3)))
      val beModApproved = JsonReader.fromString(docFilters.moderationStatusFilter(ApprovalStatus.approved, domainSet).toString)
      val beDatalensApproved = JsonReader.fromString(docFilters.datalensStatusFilter(ApprovalStatus.approved).toString)
      val beRAApproved = JsonReader.fromString(docFilters.raStatusFilter(ApprovalStatus.approved, domainSet).toString)
      val expected = j"""{ "bool": { "must": [$beModApproved, $beDatalensApproved, $beRAApproved]}}"""

      val filter = docFilters.approvalStatusFilter(ApprovalStatus.approved, domainSet, includeContextApproval = false)
      val actual = JsonReader.fromString(filter.toString)
      actual should be(expected)
    }

    "return the expected filter for rejected views if the status is rejected and we include the context" in {
      val domainSet = DomainSet(someDomains, Some(domains(3)))
      val beModRejected = JsonReader.fromString(docFilters.moderationStatusFilter(ApprovalStatus.rejected, domainSet).toString)
      val beDatalensRejected = JsonReader.fromString(docFilters.datalensStatusFilter(ApprovalStatus.rejected).toString)
      val beRARejected = JsonReader.fromString(docFilters.raStatusFilter(ApprovalStatus.rejected, domainSet).toString)
      val beRARejectedOnContext = JsonReader.fromString(docFilters.raStatusOnContextFilter(ApprovalStatus.rejected, domainSet).get.toString)
      val expected = j"""{ "bool": { "should": [$beModRejected, $beDatalensRejected, $beRARejected, $beRARejectedOnContext]}}"""

      val filter = docFilters.approvalStatusFilter(ApprovalStatus.rejected, domainSet, includeContextApproval = true)
      val actual = JsonReader.fromString(filter.toString)
      actual should be(expected)
    }

    "return the expected filter for rejected views if the status is rejected and we exclude the context" in {
      val domainSet = DomainSet(someDomains, Some(domains(3)))
      val beModRejected = JsonReader.fromString(docFilters.moderationStatusFilter(ApprovalStatus.rejected, domainSet).toString)
      val beDatalensRejected = JsonReader.fromString(docFilters.datalensStatusFilter(ApprovalStatus.rejected).toString)
      val beRARejected = JsonReader.fromString(docFilters.raStatusFilter(ApprovalStatus.rejected, domainSet).toString)
      val expected = j"""{ "bool": { "should": [$beModRejected, $beDatalensRejected, $beRARejected]}}"""

      val filter = docFilters.approvalStatusFilter(ApprovalStatus.rejected, domainSet, includeContextApproval = false)
      val actual = JsonReader.fromString(filter.toString)
      actual should be(expected)
    }

    "return the expected filter for pending views if the status is pending and we include the context" in {
      val domainSet = DomainSet(someDomains, Some(domains(3)))
      val beModPending = JsonReader.fromString(docFilters.moderationStatusFilter(ApprovalStatus.pending, domainSet).toString)
      val beDatalensPending = JsonReader.fromString(docFilters.datalensStatusFilter(ApprovalStatus.pending).toString)
      val beRAPending = JsonReader.fromString(docFilters.raStatusFilter(ApprovalStatus.pending, domainSet).toString)
      val beRAPendingOnContext = JsonReader.fromString(docFilters.raStatusOnContextFilter(ApprovalStatus.pending, domainSet).get.toString)
      val expected = j"""{ "bool": { "should": [$beModPending, $beDatalensPending, $beRAPending, $beRAPendingOnContext]}}"""

      val filter = docFilters.approvalStatusFilter(ApprovalStatus.pending, domainSet, includeContextApproval = true)
      val actual = JsonReader.fromString(filter.toString)
      actual should be(expected)
    }

    "return the expected filter for pending views if the status is pending and we exclude the context" in {
      val domainSet = DomainSet(someDomains, Some(domains(3)))
      val beModPending = JsonReader.fromString(docFilters.moderationStatusFilter(ApprovalStatus.pending, domainSet).toString)
      val beDatalensPending = JsonReader.fromString(docFilters.datalensStatusFilter(ApprovalStatus.pending).toString)
      val beRAPending = JsonReader.fromString(docFilters.raStatusFilter(ApprovalStatus.pending, domainSet).toString)
      val expected = j"""{ "bool": { "should": [$beModPending, $beDatalensPending, $beRAPending]}}"""

      val filter = docFilters.approvalStatusFilter(ApprovalStatus.pending, domainSet, includeContextApproval = false)
      val actual = JsonReader.fromString(filter.toString)
      actual should be(expected)
    }
  }

  "the socrataCategoriesFilter" should {
    "return the expected filter when there are categories given" in {
      val docFilter = docFilters.socrataCategoriesFilter(Set("cats", "dogs"))
      val domFilter = docFiltersForDomainSearch.socrataCategoriesFilter(Set("cats", "dogs"))
      val expectedDocFilter =
        j"""{ "bool" : {"should" : { "nested" : {
              "filter" : { "terms" : { "animl_annotations.categories.name.raw" : [ "cats", "dogs" ] }},
              "path" : "animl_annotations.categories"
            }}}}"""
      val expectedDomFilter =
        j"""{ "bool" : {"should" : { "nested" : {
              "filter" : { "terms" : { "animl_annotations.categories.name.raw" : [ "cats", "dogs" ] }},
              "path" : "document.animl_annotations.categories"
            }}}}"""
      val actualDocFilter = JsonReader.fromString(docFilter.toString)
      val actualDomFilter = JsonReader.fromString(domFilter.toString)
      actualDocFilter should be(expectedDocFilter)
      actualDomFilter should be(expectedDomFilter)
    }
  }

  "the socrataTagsFilter" should {
    "return the expected filter when there are tags given" in {
      val docFilter = docFilters.socrataTagsFilter(Set("cats", "dogs"))
      val domFilter = docFiltersForDomainSearch.socrataTagsFilter(Set("cats", "dogs"))
      val expectedDocFilter =
        j"""{ "bool" : {"should" : { "nested" : {
              "filter" : { "terms" : { "animl_annotations.tags.name.raw" : [ "cats", "dogs" ] }},
              "path" : "animl_annotations.tags"
            }}}}"""
      val expectedDomFilter =
        j"""{ "bool" : {"should" : { "nested" : {
              "filter" : { "terms" : { "animl_annotations.tags.name.raw" : [ "cats", "dogs" ] }},
              "path" : "document.animl_annotations.tags"
            }}}}"""
      val actualDocFilter = JsonReader.fromString(docFilter.toString)
      val actualDomFilter = JsonReader.fromString(domFilter.toString)
      actualDocFilter should be(expectedDocFilter)
      actualDomFilter should be(expectedDomFilter)
    }
  }

  "the categoryFilter" should {
    "return None if we aren't searching over domains" in {
      val docFilterNoContext = docFilters.categoryFilter(Set("cats", "dogs"), context = None)
      val docFilterWithContext = docFilters.categoryFilter(Set("cats", "dogs"), context = Some(domains(0)))
      docFilterNoContext should be(None)
      docFilterWithContext should be(None)
    }

    "return the customer category filter if we are searching over domains and we have a context" in {
      val categories = Set("cats", "dogs")
      val filter = docFiltersForDomainSearch.categoryFilter(categories, context = Some(domains(0)))
      val expectedFilter = JsonReader.fromString(docFiltersForDomainSearch.customerCategoriesFilter(categories).toString)
      val actualFilter = JsonReader.fromString(filter.get.toString)
      actualFilter should be(expectedFilter)
    }

    "return the socrata category filter if we are searching over domains and we do not have a context" in {
      val categories = Set("cats", "dogs")
      val filter = docFiltersForDomainSearch.categoryFilter(categories, context = None)
      val expectedFilter = JsonReader.fromString(docFiltersForDomainSearch.socrataCategoriesFilter(categories).toString)
      val actualFilter = JsonReader.fromString(filter.get.toString)
      actualFilter should be(expectedFilter)
    }
  }

  "the tagFilter" should {
    "return None if we aren't searching over domains" in {
      val docFilterNoContext = docFilters.tagFilter(Set("cats", "dogs"), context = None)
      val docFilterWithContext = docFilters.tagFilter(Set("cats", "dogs"), context = Some(domains(0)))
      docFilterNoContext should be(None)
      docFilterWithContext should be(None)
    }

    "return the customer tag filter if we are searching over domains and we have a context" in {
      val tags = Set("cats", "dogs")
      val filter = docFiltersForDomainSearch.tagFilter(tags, context = Some(domains(0)))
      val expectedFilter = JsonReader.fromString(docFiltersForDomainSearch.customerTagsFilter(tags).toString)
      val actualFilter = JsonReader.fromString(filter.get.toString)
      actualFilter should be(expectedFilter)
    }

    "return the socrata tag filter if we are searching over domains and we do not have a context" in {
      val tags = Set("cats", "dogs")
      val filter = docFiltersForDomainSearch.tagFilter(tags, context = None)
      val expectedFilter = JsonReader.fromString(docFiltersForDomainSearch.socrataTagsFilter(tags).toString)
      val actualFilter = JsonReader.fromString(filter.get.toString)
      actualFilter should be(expectedFilter)
    }
  }

  "the publicFilter" should {
    "return the expected filter when no params are passed" in {
      val filter = docFilters.publicFilter()
      val actual = JsonReader.fromString(filter.toString)
      val expected = j"""{"term" : {"is_public" : true} }"""
      actual should be(expected)
    }

    "return the expected filter when a 'true' param passed" in {
      val filter = docFilters.publicFilter(true)
      val actual = JsonReader.fromString(filter.toString)
      val expected = j"""{"term" : {"is_public" : true} }"""
      actual should be(expected)
    }

    "return the expected filter when a 'false' param passed" in {
      val filter = docFilters.publicFilter(false)
      val actual = JsonReader.fromString(filter.toString)
      val expected = j"""{"term" : {"is_public" : false} }"""
      actual should be(expected)
    }
  }

  "the publishedFilter" should {
    "return the expected filter when no params are passed" in {
      val filter = docFilters.publishedFilter()
      val actual = JsonReader.fromString(filter.toString)
      val expected = j"""{"term" : {"is_published" : true} }"""
      actual should be(expected)
    }

    "return the expected filter when a 'true' param passed" in {
      val filter = docFilters.publishedFilter(true)
      val actual = JsonReader.fromString(filter.toString)
      val expected = j"""{"term" : {"is_published" : true} }"""
      actual should be(expected)
    }

    "return the expected filter when a 'false' param passed" in {
      val filter = docFilters.publishedFilter(false)
      val actual = JsonReader.fromString(filter.toString)
      val expected = j"""{"term" : {"is_published" : false} }"""
      actual should be(expected)
    }
  }

  "the searchParamsFilter" should {
    "throw an unauthorizedError if there is no authenticated user looking for what is shared to another user" in {
      val searchParams = SearchParamSet(sharedTo = Some("ro-bear"))
      val user = None
      an[UnauthorizedError] should be thrownBy {
        docFilters.searchParamsFilter(searchParams, user, DomainSet())
      }
    }

    "throw an unauthorizedError if there is an authenticated user is looking for what is shared to another user" in {
      val searchParams = SearchParamSet(sharedTo = Some("ro-bear"))
      val user = Some(User("anna-belle", flags = Some(List("admin"))))
      an[UnauthorizedError] should be thrownBy {
        docFilters.searchParamsFilter(searchParams, user, DomainSet())
      }
    }

    "return the expected filter" in {
      val searchParams = SearchParamSet(
        searchQuery = SimpleQuery("search query terms"),
        domains = Some(Set("www.example.com", "test.example.com", "socrata.com")),
        searchContext = Some("www.search.me"),
        domainMetadata = Some(Set(("key", "value"))),
        categories = Some(Set("Social Services", "Environment", "Housing & Development")),
        tags = Some(Set("taxi", "art", "clowns")),
        datatypes = Some(Set("dataset")),
        user = Some("anna-belle"),
        sharedTo = Some("ro-bear"),
        attribution = Some("org"),
        parentDatasetId = Some("parent-id"),
        ids = Some(Set("id-one", "id-two")),
        license = Some("GNU GPL")
      )
      val user = User("ro-bear")
      val filter = docFilters.searchParamsFilter(searchParams, Some(user), DomainSet()).get
      val actual = JsonReader.fromString(filter.toString)
      val expected = j"""{
        "bool" :
          {
            "must" :
              [
                { "terms" : { "datatype" : [ "dataset" ] } },
                { "term" : { "owner_id" : "anna-belle" } },
                { "term" : { "shared_to" : "ro-bear" } },
                { "term" : { "attribution.raw" : "org" } },
                { "term" : { "socrata_id.parent_dataset_id" : "parent-id" } },
                { "terms" : { "_id" : [ "id-one", "id-two" ] } },
                {"bool" :{"should" :{"nested" :{"filter" :{"bool" :{"must" :[
                  {"terms" :{"customer_metadata_flattened.key.raw" :[ "key" ]}},
                  {"terms" :{"customer_metadata_flattened.value.raw" :[ "value" ]}}
                ]}}, "path" : "customer_metadata_flattened"}}}},
                { "term" : { "license" : "GNU GPL" } }
              ]
          }
      }
      """
      actual should be(expected)
    }
  }

  "the anonymousFilter" should {
    "return the expected filter" in {
      val domainSet = DomainSet((0 to 2).map(domains(_)).toSet, Some(domains(2)))
      val public = JsonReader.fromString(docFilters.publicFilter(public = true).toString)
      val published = JsonReader.fromString(docFilters.publishedFilter(published = true).toString)
      val approved = JsonReader.fromString(docFilters.approvalStatusFilter(ApprovalStatus.approved, domainSet).toString)
      val unhidden = JsonReader.fromString(docFilters.hiddenFromCatalogFilter(false).toString)
      val expected = j"""{ "bool": { "must": [$public, $published, $approved, $unhidden]}}"""
      val filter = docFilters.anonymousFilter(domainSet)
      val actual = JsonReader.fromString(filter.toString)
       actual should be(expected)
    }
  }

  "the ownedOrSharedFilter" should {
    "return the expected filter" in {
      val user = User("mooks")
      val filter = docFilters.ownedOrSharedFilter(user)
      val actual = JsonReader.fromString(filter.toString)
      val expected = j"""{
        "bool" :{"should" :[
          { "term" : { "owner_id" : "mooks" } },
          { "term" : { "shared_to" : "mooks" } }
        ]}
      }"""
      actual should be(expected)
    }
  }

  "the authFilter" should {
    val someDomains = (0 to 4).map(domains(_)).toSet
    val contextWithRA = Some(domains(3))

    "throw an UnauthorizedError if no user is given and auth is required" in  {
      an[UnauthorizedError] should be thrownBy {
        docFilters.authFilter(None, DomainSet(), true)
      }
    }

    "return None for super admins if auth is required" in {
      val user = User("mooks", flags = Some(List("admin")))
      val domainSet = DomainSet(someDomains, contextWithRA)
      val filter = docFilters.authFilter(Some(user), domainSet, true)
      filter should be(None)
    }

    "return anon (acknowleding context) and personal views for users who can view everything (but aren't super admins and don't have an authenticating domain) if auth is required" in {
      val user = User("mooks", roleName = Some("publisher"))
      val domainSet = DomainSet(someDomains, contextWithRA)
      val filter = docFilters.authFilter(Some(user), domainSet, true)
      val actual = JsonReader.fromString(filter.get.toString)
      val anonFilter = JsonReader.fromString(docFilters.anonymousFilter(domainSet, includeContextApproval = true).toString)
      val personalFilter = JsonReader.fromString(docFilters.ownedOrSharedFilter(user).toString)
      val expected = j"""{"bool": {"should": [$personalFilter, $anonFilter]}}"""
      actual should be(expected)
    }

    "return anon views (disregarding context), personal views and within-domains views for users who can view everything and do have an authenticating domain if auth is required" in {
      val user = User("mooks", authenticatingDomain = contextWithRA, roleName = Some("publisher"))
      val domainSet = DomainSet(someDomains, contextWithRA)
      val filter = docFilters.authFilter(Some(user), domainSet, true)
      val actual = JsonReader.fromString(filter.get.toString)
      val anonFilter = JsonReader.fromString(docFilters.anonymousFilter(domainSet, includeContextApproval = false).toString)
      val personalFilter = JsonReader.fromString(docFilters.ownedOrSharedFilter(user).toString)
      val withinDomainFilter = JsonReader.fromString(docFilters.domainIdFilter(Set(3)).toString)
      val expected = j"""{"bool": {"should": [$personalFilter, $anonFilter, $withinDomainFilter]}}"""
      actual should be(expected)
    }

    "return anon (acknowleding context) and personal views for users who have logged in but cannot view everything if auth is required" in {
      val user = User("mooks", authenticatingDomain = contextWithRA, roleName = Some("editor"))
      val domainSet = DomainSet(someDomains, contextWithRA)
      val filter = docFilters.authFilter(Some(user), domainSet, true)
      val actual = JsonReader.fromString(filter.get.toString)
      val anonFilter = JsonReader.fromString(docFilters.anonymousFilter(domainSet, includeContextApproval = true).toString)
      val personalFilter = JsonReader.fromString(docFilters.ownedOrSharedFilter(user).toString)
      val expected = j"""{"bool": {"should": [$personalFilter, $anonFilter]}}"""
      actual should be(expected)
    }

    "return only anon (acknowleding context) views if auth is not required" in {
      val userWhoCannotViewAll = User("mooks", roleName = Some("editor"))
      val userWhoCanViewAllButNoDomain = User("mooks", roleName = Some("publisher"))
      val userWhoCanViewAllWithDomain = User("mooks", contextWithRA, roleName = Some("publisher"))
      val superAdmin = User("mooks", flags = Some(List("admin")))
      val domainSet = DomainSet(someDomains, contextWithRA)

      val filter1 = docFilters.authFilter(Some(userWhoCannotViewAll), domainSet, false)
      val filter2 = docFilters.authFilter(Some(userWhoCanViewAllButNoDomain), domainSet, false)
      val filter3 = docFilters.authFilter(Some(userWhoCanViewAllWithDomain), domainSet, false)
      val filter4 = docFilters.authFilter(Some(superAdmin), domainSet, false)

      val actual1 = JsonReader.fromString(filter1.get.toString)
      val actual2 = JsonReader.fromString(filter2.get.toString)
      val actual3 = JsonReader.fromString(filter3.get.toString)
      val actual4 = JsonReader.fromString(filter4.get.toString)
      val anonFilter = JsonReader.fromString(docFilters.anonymousFilter(domainSet, includeContextApproval = true).toString)

      actual1 should be(anonFilter)
      actual2 should be(anonFilter)
      actual3 should be(anonFilter)
      actual4 should be(anonFilter)
    }
  }
}
