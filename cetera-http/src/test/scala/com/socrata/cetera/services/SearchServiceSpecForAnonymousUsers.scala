package com.socrata.cetera.services

import java.nio.charset.{Charset, CodingErrorAction}

import com.rojoma.json.v3.ast.{JArray, JString}
import com.rojoma.simplearm.v2._
import org.scalatest.{BeforeAndAfterAll, BeforeAndAfterEach, FunSuiteLike, Matchers}

import com.socrata.cetera.TestESData
import com.socrata.cetera.auth.AuthParams
import com.socrata.cetera.errors.DomainNotFoundError
import com.socrata.cetera.handlers.Params
import com.socrata.cetera.response.SearchResult

class SearchServiceSpecForAnonymousUsers
  extends FunSuiteLike
    with Matchers
    with TestESData
    with BeforeAndAfterAll
    with BeforeAndAfterEach {

  override protected def beforeAll(): Unit = bootstrapData()

  override protected def afterAll(): Unit = {
    removeBootstrapData()
    client.close()
    httpClient.close()
  }

  val browseParams = Map(
    "public" -> "true",
    "published" -> "true",
    "approval_status" -> "approved",
    "explicitly_hidden" -> "false"
  ).mapValues(Seq(_))

  test("a basic search without auth on a basic domain (no VM, no RA) should return the expected results") {
    val basicDomain = domains(0).domainCname
    val domain0Docs = docs.filter(d => d.socrataId.domainId == 0)
    val allPossibleResults = getAllPossibleResults()
    val approvedFxfs = allPossibleResults.filter(isApproved).map(fxfs)
    val expectedFxfs = fxfs(domain0Docs.filter(d =>
      d.isPublic && d.isPublished && !d.isHiddenFromCatalog && approvedFxfs.contains(d.socrataId.datasetId)))

    val params = Map("search_context" -> basicDomain, "domains" -> basicDomain).mapValues(Seq(_))
    val actualFxfs = fxfs(service.doSearch(params, AuthParams(), None, None)._2)
    expectedFxfs shouldNot be('empty)
    actualFxfs should contain theSameElementsAs expectedFxfs
  }

  test("a basic search without auth on a moderated domain (but no RA) should return the expected results") {
    val moderatedDomain = domains(1).domainCname
    val domain1Docs = docs.filter(d => d.socrataId.domainId == 1)
    val allPossibleResults = getAllPossibleResults()
    val approvedFxfs = allPossibleResults.filter(isApproved).map(fxfs)
    val expectedFxfs = fxfs(domain1Docs.filter(d =>
      d.isPublic && d.isPublished && !d.isHiddenFromCatalog && approvedFxfs.contains(d.socrataId.datasetId)))

    val params = Map("search_context" -> moderatedDomain, "domains" -> moderatedDomain).mapValues(Seq(_))
    val actualFxfs = fxfs(service.doSearch(params, AuthParams(), None, None)._2)
    expectedFxfs shouldNot be('empty)
    actualFxfs should contain theSameElementsAs expectedFxfs
  }

  test("a basic search without auth on an RA-enabled domain (but no VM) should return the expected results") {
    val raEnabledDomain = domains(2).domainCname
    val domain2Docs = docs.filter(d => d.socrataId.domainId == 2)
    val allPossibleResults = getAllPossibleResults()
    val approvedFxfs = allPossibleResults.filter(isApproved).map(fxfs)
    val expectedFxfs = fxfs(domain2Docs.filter(d =>
      d.isPublic && d.isPublished && !d.isHiddenFromCatalog && approvedFxfs.contains(d.socrataId.datasetId)))

    val params = Map("search_context" -> raEnabledDomain, "domains" -> raEnabledDomain).mapValues(Seq(_))
    val actualFxfs = fxfs(service.doSearch(params, AuthParams(), None, None)._2)
    expectedFxfs shouldNot be('empty)
    actualFxfs should contain theSameElementsAs expectedFxfs
  }

  test("a basic search without auth on an RA & VM-enabled domain should return the expected results") {
    val raAndVmEnabledDomain = domains(3).domainCname
    val domain3Docs = docs.filter(d => d.socrataId.domainId == 3)
    val allPossibleResults = getAllPossibleResults()
    val approvedFxfs = allPossibleResults.filter(isApproved).map(fxfs)
    val expectedFxfs = fxfs(domain3Docs.filter(d =>
      d.isPublic && d.isPublished && !d.isHiddenFromCatalog && approvedFxfs.contains(d.socrataId.datasetId)))

    val params = Map("search_context" -> raAndVmEnabledDomain, "domains" -> raAndVmEnabledDomain).mapValues(Seq(_))
    val actualFxfs = fxfs(service.doSearch(params, AuthParams(), None, None)._2)
    expectedFxfs shouldNot be('empty)
    actualFxfs should contain theSameElementsAs expectedFxfs
  }

  test("searching across all domains when auth is not required returns anonymously viewable views on unlocked domains") {
    val res = service.doSearch(allDomainsParams, AuthParams(), None, None)
    fxfs(res._2) should contain theSameElementsAs anonymouslyViewableDocIds
  }

  test("search with a non-existent search_context throws a DomainNotFoundError") {
    val params = Map(
      "domains" -> "opendata-demo.socrata.com",
      "search_context" -> "bad-domain.com"
    ).mapValues(Seq(_))
    intercept[DomainNotFoundError] {
      service.doSearch(params, AuthParams(), None, None)
    }
  }

  test("search response contains pretty and perma links") {
    service.doSearch(Map.empty, AuthParams(), None, None)._2.results.foreach { r =>
      val dsid = r.resource.dyn.id.!.asInstanceOf[JString].string

      val perma = "(d|stories/s|view)"
      val alphanum = "[\\p{L}\\p{N}\\-]+" // all of the test data have proper categories and names
    val pretty = s"$alphanum/$alphanum"

      r.permalink.string should endWith regex s"/$perma/$dsid"
      r.link.string should endWith regex s"/$pretty/$dsid"
    }
  }

  test("search response without a searchContext should have the correct set of documents") {
    val customerDomainIds = domains.filter(d => d.isCustomerDomain).map(_.domainId)
    val anonymousCustomerDocs = anonymouslyViewableDocs.filter(d => customerDomainIds.contains(d.socrataId.domainId))
    val expectedFxfs = anonymousCustomerDocs.map(_.socrataId.datasetId)

    // this shows that:
    //   * rejected and pending views don't show up regardless of domain setting
    //   * that the ES type returned includes only documents (i.e. no domains)
    //   * that non-customer domains don't show up
    val (_, res, _, _) = service.doSearch(Map.empty, AuthParams(), None, None)
    val actualFxfs = fxfs(res)
    actualFxfs should contain theSameElementsAs expectedFxfs
  }

  test("domainBoosts are respected") {
    val params = Map(
      Params.domains -> "petercetera.net,annabelle.island.net",
      s"${Params.boostDomains}[annabelle.island.net]" -> "0.0",
      Params.showScore -> "true"
    )
    val (_, res, _, _) = service.doSearch(params.mapValues(Seq(_)), AuthParams(), None, None)
    val metadata = res.results.map(_.metadata)
    val annabelleRes = metadata.filter(_.domain == "annabelle.island.net")
    annabelleRes.foreach { r =>
      r.score.get should be(0.0)
    }
  }

  test("private documents should always be hidden") {
    val expectedFxfs = Set.empty
    val (_, res, _, _) = service.doSearch(Map(
      Params.domains -> "petercetera.net",
      Params.searchContext -> "petercetera.net",
      Params.q -> "private"
    ).mapValues(Seq(_)), AuthParams(), None, None)
    val actualFxfs = fxfs(res)
    actualFxfs should contain theSameElementsAs expectedFxfs
  }

  test("unpublished documents should always be hidden") {
    val expectedFxfs = Set.empty
    val (_, res, _, _) = service.doSearch(Map(
      Params.domains -> "petercetera.net",
      Params.searchContext -> "petercetera.net",
      Params.q -> "unpublished"
    ).mapValues(Seq(_)), AuthParams(), None, None)
    val actualFxfs = fxfs(res)
    actualFxfs should contain theSameElementsAs expectedFxfs
  }

  test("hidden documents should be hidden when auth isn't required") {
    val hiddenDoc = docs(4)
    val (_, res, _, _) = service.doSearch(Map(
      Params.ids -> hiddenDoc.socrataId.datasetId
    ).mapValues(Seq(_)), AuthParams(), None, None)
    val actualFxfs = fxfs(res)
    // ensure the hidden doc didn't come back
    actualFxfs should be('empty)
    // ensure that's b/c it's hidden and not b/c it is private or unpublished
    hiddenDoc.isPublic should be(true)
    hiddenDoc.isPublished should be(true)
  }

  test("not_moderated data federated to a moderated domain should not be in the response") {
    // the domain params will limit us to fxfs 0,4,8 and 1,5,9
    val params = Map(
      "domains" -> "opendata-demo.socrata.com,petercetera.net",
      "search_context" -> "opendata-demo.socrata.com"
    ).mapValues(Seq(_))
    // of those fxfs, only show: fxf-1 is approved and fxf-8 is a default view
    val expectedFxfs = Set("fxf-1", "fxf-8", "zeta-0007")
    val res = service.doSearch(params, AuthParams(), None, None)._2
    val actualFxfs = fxfs(res)
    actualFxfs should contain theSameElementsAs expectedFxfs
  }

  test("if a domain has routing & approval, only parent domain approved datasets should show up") {
    val routedDomain = domains(2)
    val params = Map("domains" -> routedDomain.domainCname).mapValues(Seq(_))
    val domain2Docs = docs.filter(d => d.socrataId.domainId == 2)
    // the filters below to find expected 4x4s are only correct because of the limited set of data on domain 2
    val expectedFxfs = domain2Docs.filter(d => d.isApprovedByParentDomain && !d.isHiddenFromCatalog).map(_.socrataId.datasetId)
    val res = service.doSearch(params, AuthParams(), None, None)._2
    val actualFxfs = fxfs(res)
    actualFxfs should contain theSameElementsAs expectedFxfs
  }

  test("if a domain is locked and no auth is provided, nothing should come back") {
    val lockedDomain = domains(8).domainCname
    val params = Map(
      "domains" -> lockedDomain,
      "search_context" -> lockedDomain
    ).mapValues(Seq(_))
    val res = service.doSearch(params, AuthParams(), None, None)._2
    val actualFxfs = fxfs(res)
    // ensure nothing comes back
    actualFxfs should be('empty)

    // ensure something could have come back
    val docFrom8 = docs(20)
    docFrom8.socrataId.domainId should be(domains(8).domainId)
    docFrom8.isPublic should be(true)
    docFrom8.isPublished should be(true)
    docFrom8.isApprovedByParentDomain should be(true)
  }

  test("if a search context has routing & approval, only datasets approved by that domain too should show up") {
    val routedDomain2 = domains(2)
    val routedDomain3 = domains(3)
    val params = Map(
      "domains" -> s"${routedDomain2.domainCname},${routedDomain3.domainCname}",
      "search_context" -> routedDomain3.domainCname
    ).mapValues(Seq(_))
    val domain2Or3Docs = docs.filter(d => d.socrataId.domainId == 2 || d.socrataId.domainId == 3)
    val expectedFxfs = domain2Or3Docs.filter(d =>
      d.isApprovedByParentDomain && d.isPublic && (d.isDefaultView || d.isVmApproved)
        && d.isRaApproved(3)).map(_.socrataId.datasetId)
    val res = service.doSearch(params, AuthParams(), None, None)._2
    val actualFxfs = fxfs(res)
    actualFxfs should contain theSameElementsAs expectedFxfs
  }

  test("if a user is provided, only anonymously viewabled datasets owned by that user should show up") {
    val params = Map("for_user" -> "robin-hood").mapValues(Seq(_))
    val expectedFxfs = anonymouslyViewableDocs.filter(d => d.ownerId == "robin-hood").map(_.socrataId.datasetId)
    val res = service.doSearch(params, AuthParams(), None, None)._2
    val actualFxfs = fxfs(res)
    actualFxfs should contain theSameElementsAs expectedFxfs
  }

  // TODO: consider searching for datasets by user screen name; using new custom analyzer
  ignore("if a user's name is queried, datasets with a matching owner:screen_name should show up") {
    val params = Map("q" -> "John").mapValues(Seq(_))
    val expectedFxfs = Set("zeta-0002", "zeta-0005")
    val res = service.doSearch(params, AuthParams(), None, None)._2
    val actualFxfs = fxfs(res)
    actualFxfs should contain theSameElementsAs expectedFxfs
  }

  test("if a parent dataset is provided, response should only include anonymously viewable views derived from that dataset") {
    val params = Map(Params.derivedFrom -> "fxf-0").mapValues(Seq(_))
    val expectedFxfs = anonymouslyViewableDocs.filter(d => d.socrataId.parentDatasetId.getOrElse(Set.empty).contains("fxf-0")).map(_.socrataId.datasetId)
    val res = service.doSearch(params, AuthParams(), None, None)._2
    val actualFxfs = fxfs(res)
    actualFxfs should contain theSameElementsAs expectedFxfs
  }

  test("categories filter should be case insensitive") {
    val paramsTitleCase = Map(
      "domains" -> Seq("petercetera.net"),
      "categories" -> Seq("Personal")
    )
    val paramsLowerCase = paramsTitleCase.mapValues(_.map(_.toLowerCase))
    val paramsUpperCase = paramsTitleCase.mapValues(_.map(_.toUpperCase))

    val (_, resultsTitleCase, _, _) = service.doSearch(paramsTitleCase, AuthParams(), None, None)
    val (_, resultsLowerCase, _, _) = service.doSearch(paramsLowerCase, AuthParams(), None, None)
    val (_, resultsUpperCase, _, _) = service.doSearch(paramsUpperCase, AuthParams(), None, None)

    resultsTitleCase.results should contain theSameElementsAs resultsLowerCase.results
    resultsTitleCase.results should contain theSameElementsAs resultsUpperCase.results
  }

  test("custom domain categories filter should be case insensitive") {
    val paramsTitleCase = Map(
      "domains" -> Seq("petercetera.net"),
      "search_context" -> Seq("petercetera.net"),
      "categories" -> Seq("Alpha")
    )
    val paramsLowerCase = paramsTitleCase.mapValues(_.map(_.toLowerCase))
    val paramsUpperCase = paramsTitleCase.mapValues(_.map(_.toUpperCase))

    val (_, resultsTitleCase, _, _) = service.doSearch(paramsTitleCase, AuthParams(), None, None)
    val (_, resultsLowerCase, _, _) = service.doSearch(paramsLowerCase, AuthParams(), None, None)
    val (_, resultsUpperCase, _, _) = service.doSearch(paramsUpperCase, AuthParams(), None, None)

    resultsTitleCase.results should contain theSameElementsAs resultsLowerCase.results
    resultsTitleCase.results should contain theSameElementsAs resultsUpperCase.results
  }

  test("tags filter should be case insensitive") {
    val paramsTitleCase = Map(
      "domains" -> Seq("petercetera.net"),
      "tags" -> Seq("Happy")
    )
    val paramsLowerCase = paramsTitleCase.mapValues(_.map(_.toLowerCase))
    val paramsUpperCase = paramsTitleCase.mapValues(_.map(_.toUpperCase))

    val (_, resultsTitleCase, _, _) = service.doSearch(paramsTitleCase, AuthParams(), None, None)
    val (_, resultsLowerCase, _, _) = service.doSearch(paramsLowerCase, AuthParams(), None, None)
    val (_, resultsUpperCase, _, _) = service.doSearch(paramsUpperCase, AuthParams(), None, None)

    resultsTitleCase.results should contain theSameElementsAs resultsLowerCase.results
    resultsTitleCase.results should contain theSameElementsAs resultsUpperCase.results
  }

  test("custom domain tags filter should be case insensitive") {
    val paramsTitleCase = Map(
      "domains" -> Seq("petercetera.net"),
      "search_context" -> Seq("petercetera.net"),
      "tags" -> Seq("1-One")
    )
    val paramsLowerCase = paramsTitleCase.mapValues(_.map(_.toLowerCase))
    val paramsUpperCase = paramsTitleCase.mapValues(_.map(_.toUpperCase))

    val (_, resultsTitleCase, _, _) = service.doSearch(paramsTitleCase, AuthParams(), None, None)
    val (_, resultsLowerCase, _, _) = service.doSearch(paramsLowerCase, AuthParams(), None, None)
    val (_, resultsUpperCase, _, _) = service.doSearch(paramsUpperCase, AuthParams(), None, None)

    resultsTitleCase.results should contain theSameElementsAs resultsLowerCase.results
    resultsTitleCase.results should contain theSameElementsAs resultsUpperCase.results
  }

  test("searching for a category should include partial phrase matches") {
    val params = Map(
      "search_context" -> "petercetera.net",
      "domains" -> "petercetera.net",
      "q" -> "Alpha"
    ).mapValues(Seq(_))

    val expectedFxfs = Set("fxf-0", "fxf-8")
    val res = service.doSearch(params, AuthParams(), None, None)._2

    val actualFxfs = fxfs(res)
    actualFxfs should contain theSameElementsAs expectedFxfs
  }

  // TODO: double check that partial phrase match on a category/tag filter is appropriate
  test("filtering by a category should include partial phrase matches") {
    val params = Map(
      "search_context" -> "petercetera.net",
      "domains" -> "petercetera.net",
      "categories" -> "Alpha"
    ).mapValues(Seq(_))

    val expectedFxfs = Set("fxf-0", "fxf-8")
    val res = service.doSearch(params, AuthParams(), None, None)._2

    val actualFxfs = fxfs(res)
    actualFxfs should contain theSameElementsAs expectedFxfs
  }

  test("filtering by a category should NOT include individual term matches") {
    val params = Map(
      "search_context" -> "petercetera.net",
      "domains" -> "petercetera.net",
      // the full category is "Alpha to Omega" and we used to include matches on any one of the terms e.g. Alpha
      "categories" -> "Alpha Beta Gaga"
    ).mapValues(Seq(_))

    val expectedFxfs = Set.empty
    val res = service.doSearch(params, AuthParams(), None, None)._2

    val actualFxfs = fxfs(res)
    actualFxfs should contain theSameElementsAs expectedFxfs
  }

  test("sorting by name works") {
    val params = Map("order" -> Seq("name"))
    val (_, results, _, _) = service.doSearch(params, AuthParams(), None, None)
    val expected = results.results.map(_.resource.dyn("name").!.asInstanceOf[JString].string).sorted.head
    val firstResult = results.results.head.resource.dyn("name").? match {
      case Right(n) => n should be(JString(expected))
      case Left(_) => fail("resource had no name!")
    }
  }

  test("sorting by name DESC works") {
    val params = Map("order" -> Seq("name DESC"))
    val (_, results, _, _) = service.doSearch(params, AuthParams(), None, None)
    val expected = results.results.map(_.resource.dyn("name").!.asInstanceOf[JString].string).sorted.last
    val firstResult = results.results.head.resource.dyn("name").? match {
      case Right(n) => n should be(JString(expected))
      case Left(_) => fail("resource had no name!")
    }
  }

  test("filtering by attribution works") {
    val params = Map("attribution" -> Seq("The Merry Men"))
    val (_, results, _, _) = service.doSearch(params, AuthParams(), None, None)
    val expectedFxfs = Set("zeta-0007")
    val actualFxfs = fxfs(results)
    actualFxfs should contain theSameElementsAs expectedFxfs
  }

  test("filtering by attribution is case sensitive") {
    val params = Map("attribution" -> Seq("the merry men"))
    val (_, results, _, _) = service.doSearch(params, AuthParams(), None, None)
    val expectedFxfs = Set.empty
    val actualFxfs = fxfs(results)
    actualFxfs should contain theSameElementsAs expectedFxfs
  }

  test("searching for attribution via keyword searches should include individual term matches regardless of case") {
    val params = Map("q" -> Seq("merry men"))
    val (_, results, _, _) = service.doSearch(params, AuthParams(), None, None)
    val expectedFxfs = Set("zeta-0007")
    val actualFxfs = fxfs(results)
    actualFxfs should contain theSameElementsAs expectedFxfs
  }

  test("attribution is included in the resulting resource") {
    val params = Map("q" -> Seq("merry men"))
    val (_, results, _, _) = service.doSearch(params, AuthParams(), None, None)
    results.results.headOption.map { case SearchResult(resource, _, _, _, _, _) =>
      resource.dyn.attribution.!.asInstanceOf[JString].string
    } should be(Some("The Merry Men"))
  }

  test("filtering by provenance works and the resulting resource has a 'provenance' field") {
    val params = Map("provenance" -> Seq("official"))
    val (_, results, _, _) = service.doSearch(params, AuthParams(), None, None)
    results.results.headOption.map { case SearchResult(resource, _, _, _, _, _) =>
      resource.dyn.provenance.!.asInstanceOf[JString].string
    } should be(Some("official"))
  }

  test("filtering by license works and the resulting metadata has a 'license' field") {
    val params = Map("license" -> Seq("Academic Free License"))
    val (_, results, _, _) = service.doSearch(params, AuthParams(), None, None)
    results.results.headOption.map { case SearchResult(_, _, metadata, _, _, _) =>
      metadata.license.get
    } should be(Some("Academic Free License"))
  }

  test("filtering by license should be exact") {
    val params = Map("license" -> Seq("Free License"))
    val (_, results, _, _) = service.doSearch(params, AuthParams(), None, None)
    results.results.headOption.map { case SearchResult(_, _, metadata, _, _, _) =>
      metadata.license.get
    } should be(None)
  }

  test("filtering by license should be case sensitive") {
    val params = Map("license" -> Seq("academic free license"))
    val (_, results, _, _) = service.doSearch(params, AuthParams(), None, None)
    results.results.headOption.map { case SearchResult(_, _, metadata, _, _, _) =>
      metadata.license.get
    } should be(None)
  }

  test("a query that results in a dataset with a license should return metadata with a 'license' field") {
    val params = Map("q" -> Seq("A dataset with a multiword title"))
    val (_, results, _, _) = service.doSearch(params, AuthParams(), None, None)
    results.results.headOption.map { case SearchResult(_, _, metadata, _, _, _) =>
      metadata.license.get
    } should be(Some("Academic Free License"))
  }

  test("passing a datatype boost should have no effect on the size of the result set") {
    val (_, results, _, _) = service.doSearch(Map.empty, AuthParams(), None, None)
    val params = Map("boostFiles" -> Seq("2.0"))
    val (_, resultsBoosted, _, _) = service.doSearch(params, AuthParams(), None, None)
    resultsBoosted.resultSetSize should be(results.resultSetSize)
  }

  test("giving a datatype a boost of >1 should promote assets of that type to the top") {
    val params = Map("boostStories" -> Seq("10.0"))
    val (_, results, _, _) = service.doSearch(params, AuthParams(), None, None)
    val resultTypes = results.results.map(_.resource.dyn.`type`.!.asInstanceOf[JString].string)
    val topResultType = resultTypes.headOption
    topResultType should be(Some("story"))
  }

  test("giving a datatype a boost of <<1 should demote assets of that type to the bottom") {
    val params = Map("boostStories" -> Seq(".0000001"))
    val (_, results, _, _) = service.doSearch(params, AuthParams(), None, None)
    val resultTypes = results.results.map(_.resource.dyn.`type`.!.asInstanceOf[JString].string)
    val lastResultType = resultTypes.last
    lastResultType should be("story")
  }

  test("preview_image_url should be included in the search result when available") {
    val params = Map("ids" -> Seq("zeta-0007"))
    val (_, results, _, _) = service.doSearch(params, AuthParams(), None, None)
    val resultPreviewImageUrls = results.results.map(_.previewImageUrl.map(_.asInstanceOf[JString].string))
    val firstPreviewImageUrl = resultPreviewImageUrls.headOption.flatten
    firstPreviewImageUrl should be(Some("https://petercetera.net/views/zeta-0007/files/123456789"))
  }

  test("preview_image_url should be None in the search result when not available") {
    val params = Map("ids" -> Seq("fxf-0"))
    val (_, results, _, _) = service.doSearch(params, AuthParams(), None, None)
    val resultPreviewImageUrls = results.results.map(_.previewImageUrl.map(_.asInstanceOf[JString].string))
    val firstPreviewImageUrl = resultPreviewImageUrls.headOption.flatten
    firstPreviewImageUrl should be(None)
  }

  test("no results should come back if asked for a non-existent 4x4") {
    val params = Map("ids" -> Seq("fake-4x4"))
    val (_, results, _, _) = service.doSearch(params, AuthParams(), None, None)
    val actualFxfs = fxfs(results)
    actualFxfs should be('empty)
  }

  test("adding on the set of 'browse' params should not change the set of anonymously viewable results in the ODN scenario") {
    val (_, results, _, _) = service.doSearch(Map.empty, AuthParams(), None, None)
    val (_, resultsWithRedundantParams, _, _) = service.doSearch(browseParams, AuthParams(), None, None)

    val actualFxfs = fxfs(resultsWithRedundantParams)
    val expectedFxfs = fxfs(results)
    actualFxfs should contain theSameElementsAs (expectedFxfs)
  }

  test("adding on the set of 'browse' params should not change the set of anonymously viewable results when the search_context and domains are unmoderated") {
    val unmoderatedDomains = domains.filter(!_.moderationEnabled).map(_.domainCname)
    val params = Map("search_context" -> Seq(domains(0).domainCname), "domains" -> unmoderatedDomains)
    val (_, results, _, _) = service.doSearch(params, AuthParams(), None, None)
    val (_, resultsWithRedundantParams, _, _) = service.doSearch(params ++ browseParams, AuthParams(), None, None)

    val actualFxfs = fxfs(resultsWithRedundantParams)
    val expectedFxfs = fxfs(results)
    actualFxfs should contain theSameElementsAs (expectedFxfs)
  }

  test("adding on the set of 'browse' params should not change the set of anonymously viewable results when the search_context and domains are moderated") {
    val moderatedDomains = domains.filter(_.moderationEnabled).map(_.domainCname)
    val params = Map("search_context" -> Seq(domains(1).domainCname), "domains" -> moderatedDomains)
    val (_, results, _, _) = service.doSearch(params, AuthParams(), None, None)
    val (_, resultsWithRedundantParams, _, _) = service.doSearch(params ++ browseParams, AuthParams(), None, None)

    val actualFxfs = fxfs(resultsWithRedundantParams)
    val expectedFxfs = fxfs(results)
    actualFxfs should contain theSameElementsAs (expectedFxfs)
  }

  test("adding on the set of 'browse' params should not change the set of anonymously viewable results when the search_context is unmoderated, and the domains are moderated") {
    val moderatedDomains = domains.filter(_.moderationEnabled).map(_.domainCname)
    val params = Map("search_context" -> Seq(domains(0).domainCname), "domains" -> moderatedDomains)
    val (_, results, _, _) = service.doSearch(params, AuthParams(), None, None)
    val (_, resultsWithRedundantParams, _, _) = service.doSearch(params ++ browseParams, AuthParams(), None, None)

    val actualFxfs = fxfs(resultsWithRedundantParams)
    val expectedFxfs = fxfs(results)
    actualFxfs should contain theSameElementsAs (expectedFxfs)
  }

  test("adding on the set of 'browse' params should not change the set of anonymously viewable results when the search_context is moderated, and the domains are unmoderated") {
    val unmoderatedDomains = domains.filter(!_.moderationEnabled).map(_.domainCname)
    val params = Map("search_context" -> Seq(domains(1).domainCname), "domains" -> unmoderatedDomains)
    val (_, results, _, _) = service.doSearch(params, AuthParams(), None, None)
    val (_, resultsWithRedundantParams, _, _) = service.doSearch(params ++ browseParams, AuthParams(), None, None)

    val actualFxfs = fxfs(resultsWithRedundantParams)
    val expectedFxfs = fxfs(results)
    actualFxfs should contain theSameElementsAs (expectedFxfs)
  }

  test("adding on a public=false param should empty out the set of anonymously viewable results") {
    val params = Map("public" -> "false").mapValues(Seq(_))
    val (_, results, _, _) = service.doSearch(params, AuthParams(), None, None)
    val actualFxfs = fxfs(results)
    actualFxfs should be('empty)
  }

  test("adding on a published=false param should empty out the set of anonymously viewable results") {
    val params = Map("published" -> "false").mapValues(Seq(_))
    val (_, results, _, _) = service.doSearch(params, AuthParams(), None, None)
    val actualFxfs = fxfs(results)
    actualFxfs should be('empty)
  }

  test("adding on an approval_status=pending param should empty out the set of anonymously viewable results") {
    val params = Map("approval_status" -> "pending").mapValues(Seq(_))
    val (_, results, _, _) = service.doSearch(params, AuthParams(), None, None)
    val actualFxfs = fxfs(results)
    actualFxfs should be('empty)
  }

  test("adding on an approval_status=rejected param should empty out the set of anonymously viewable results") {
    val params = Map("approval_status" -> "pending").mapValues(Seq(_))
    val (_, results, _, _) = service.doSearch(params, AuthParams(), None, None)
    val actualFxfs = fxfs(results)
    actualFxfs should be('empty)
  }

  test("adding on a explicitly_hidden=true param should empty out the set of anonymously viewable results") {
    val params = Map("explicitly_hidden" -> "true").mapValues(Seq(_))
    val (_, results, _, _) = service.doSearch(params, AuthParams(), None, None)
    val actualFxfs = fxfs(results)
    actualFxfs should be('empty)
  }

  test("searching with the 'q' param finds no items where q matches the private metadata") {
    val privateValue = "Cheetah Corp."
    val params = allDomainsParams ++ Map("q" -> privateValue).mapValues(Seq(_))
    val res = service.doSearch(params, AuthParams(), None, None)
    val actualFxfs = fxfs(res._2)
    actualFxfs should be('empty)

    // confirm there were documents that were excluded.
    anonymouslyViewableDocs.find(_.socrataId.datasetId == "fxf-8").get.privateCustomerMetadataFlattened.exists(_.value == privateValue
    ) should be(true)
  }

  test("searching with a private metadata k/v pair param finds no items if the user doesn't own/share the doc") {
    val privateKey = "Secret domain 0 cat organization"
    val privateValue = "Cheetah Corp."
    val params = allDomainsParams ++ Map(privateKey -> Seq(privateValue))
    val res = service.doSearch(params, AuthParams(), None, None)
    val actualFxfs = fxfs(res._2)
    actualFxfs should be('empty)

    // confirm there were documents that were excluded.
    anonymouslyViewableDocs.find(_.socrataId.datasetId == "fxf-8").get.privateCustomerMetadataFlattened.exists(m =>
      m.value == privateValue && m.key == privateKey) should be(true)
  }

  test("filtering by column names works") {
    val params = Map("column_names[]" -> Seq("first_name"))
    val (_, results, _, _) = service.doSearch(params, AuthParams(), None, None)
    results.results.headOption.flatMap { case SearchResult(resource, _, _, _, _, _) =>
      resource.dyn.columns_name.!.asInstanceOf[JArray] match {
        case JArray(elems) => elems.headOption.map(_.asInstanceOf[JString].string)
        case _ => None
      }
    } should be(Some("first_name"))
  }

  test("searching for column names works via the 'q' param works") {
    val params = Map("q" -> Seq("first_name"))
    val (_, results, _, _) = service.doSearch(params, AuthParams(), None, None)
    results.results.headOption.flatMap { case SearchResult(resource, _, _, _, _, _) =>
      resource.dyn.columns_name.!.asInstanceOf[JArray] match {
        case JArray(elems) => elems.headOption.map(_.asInstanceOf[JString].string)
        case _ => None
      }
    } should be(Some("first_name"))
  }
}
