package com.socrata.cetera.services

import org.scalatest.{BeforeAndAfterAll, BeforeAndAfterEach, FunSuiteLike, Matchers}

import com.socrata.cetera.TestESData
import com.socrata.cetera.auth.AuthParams
import com.socrata.cetera.errors.DomainNotFoundError
import com.socrata.cetera.handlers.Params
import com.socrata.cetera.response.Metadata
import com.socrata.cetera.types.{ApprovalStatus, FlattenedApproval}

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

  val anonymousDomain0Docs = dom0Docs.filter(d => anonymouslyViewableDocIds.contains(d.socrataId.datasetId))
  val baseOpenMetadata = Metadata(domain = "change_me", isPublic = Some(true), isPublished = Some(true), isHidden = Some(false), visibleToAnonymous = Some(true))

  def testResultSet(params: Map[String, Seq[String]], expectedFxfs: Option[Seq[String]]): Unit = {
    val res = fxfs(service.doSearch(params, AuthParams(), None, None)._2)
    expectedFxfs match {
      case None => res should be('empty)
      case Some(fxfs) =>
        res shouldNot be('empty)
        res should contain theSameElementsAs fxfs
    }
  }

  def testCaseInsensitivity(params: Map[String, Seq[String]], expectedFxfs: Seq[String]): Unit = {
    val paramsLowerCase = params.mapValues(_.map(_.toLowerCase))
    val paramsUpperCase = params.mapValues(_.map(_.toUpperCase))

    val (_, resultsTitleCase, _, _) = service.doSearch(params, AuthParams(), None, None)
    val (_, resultsLowerCase, _, _) = service.doSearch(paramsLowerCase, AuthParams(), None, None)
    val (_, resultsUpperCase, _, _) = service.doSearch(paramsUpperCase, AuthParams(), None, None)

    resultsTitleCase.results should contain theSameElementsAs resultsLowerCase.results
    resultsTitleCase.results should contain theSameElementsAs resultsUpperCase.results

    testResultSet(params, Some(expectedFxfs))
  }

  def testMetaData(domCname: String, fxf: String, expectedMetadata: Metadata) = {
    val params = Map(
      Params.domains -> domCname,
      Params.showVisibility -> "true",
      Params.ids -> fxf
    )
    val res = service.doSearch(params.mapValues(Seq(_)), AuthParams(), None, None)._2.results
    res.length should be(1)
    res(0).metadata should be(expectedMetadata)
  }

  // the idea here is give a fxf from the domain, not the context, so the context's rules apply to the returned result
  def testContextMetaData(contextName: String, domainName: String, fxf: String, expectedMetadata: Metadata) = {
    val params = Map(
      Params.domains -> s"$contextName,$domainName",
      Params.searchContext -> contextName,
      Params.showVisibility -> "true",
      Params.ids -> fxf
    )
    val res = service.doSearch(params.mapValues(Seq(_)), AuthParams(), None, None)._2.results
    res.length should be(1)
    res(0).metadata should be(expectedMetadata)
  }

  //////////////////////////////////////////////////
  // General searches
  //////////////////////////////////////////////////

  test("a basic search on a fontana domain should return the expected results") {
    val expectedFxfs = fxfs(dom0Docs.filter(d => anonymouslyViewableDocIds.contains(d.socrataId.datasetId)))
    val params = Map("search_context" -> fontanaDomain.cname, "domains" -> fontanaDomain.cname).mapValues(Seq(_))
    testResultSet(params, Some(expectedFxfs))
  }

  test("a basic search on a VM domain should return the expected results") {
    val expectedFxfs = fxfs(dom1Docs.filter(d => anonymouslyViewableDocIds.contains(d.socrataId.datasetId)))
    val params = Map("search_context" -> vmDomain.cname, "domains" -> vmDomain.cname).mapValues(Seq(_))
    testResultSet(params, Some(expectedFxfs))
  }

  test("a basic search on an RA domain should return the expected results") {
    val expectedFxfs = fxfs(dom2Docs.filter(d => anonymouslyViewableDocIds.contains(d.socrataId.datasetId)))
    val params = Map("search_context" -> raDomain.cname, "domains" -> raDomain.cname).mapValues(Seq(_))
    testResultSet(params, Some(expectedFxfs))
  }

  test("a basic search on an VM + RA domain should return the expected results") {
    val expectedFxfs = fxfs(dom3Docs.filter(d => anonymouslyViewableDocIds.contains(d.socrataId.datasetId)))
    val params = Map("search_context" -> vmRaDomain.cname, "domains" -> vmRaDomain.cname).mapValues(Seq(_))
    testResultSet(params, Some(expectedFxfs))
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
      val dsid = r.resource.id

      val perma = "(d|stories/s|view)"
      val alphanum = "[\\p{L}\\p{N}\\-]+" // all of the test data have proper categories and names
    val pretty = s"$alphanum/$alphanum"

      r.permalink should endWith regex s"/$perma/$dsid"
      r.link should endWith regex s"/$pretty/$dsid"
    }
  }

  test("visibility info on a fontana domain") {
    val approvals = Some(Vector(FlattenedApproval("publicize", "approved", "2017-10-31T12:00:00.000Z", "robin-hood", "Robin Hood", None, None,
      Some(false), Some("2017-11-01T12:00:00.000Z"), Some("honorable.sheriff"), Some("The Honorable Sheriff of Nottingham"), None)))
    val expectedMetadata = baseOpenMetadata.copy(domain = fontanaDomain.cname, approvals = approvals)
    testMetaData(fontanaDomain.cname, "d0-v0", expectedMetadata)
  }

  test("visibility info on a VM domain") {
    val expectedMetadata = baseOpenMetadata.copy(domain = vmDomain.cname, isModerationApproved = Some(true), moderationStatus = Some("approved"))
    testMetaData(vmDomain.cname, "d1-v0", expectedMetadata)
  }

  test("visibility info on an RA domain") {
    val expectedMetadata = baseOpenMetadata.copy(domain = raDomain.cname, isRoutingApproved = Some(true), routingStatus = Some("approved"))
    testMetaData(raDomain.cname, "d2-v2", expectedMetadata)
  }

  test("visibility info on a VM + RA domain") {
    val expectedMetadata = baseOpenMetadata.copy(domain = vmRaDomain.cname, isModerationApproved = Some(true), isRoutingApproved = Some(true),
      moderationStatus = Some("approved"), routingStatus = Some("approved"))
    testMetaData(vmRaDomain.cname, "d3-v3", expectedMetadata)
  }

  // views from fontana domains are special - their status isn't changed by the context, so their vis info should be constant regardless of context
  test("visibility info for a fontana domain with a VM context") {
    val approvals = Some(Vector(FlattenedApproval("publicize", "approved", "2017-10-31T12:00:00.000Z", "robin-hood", "Robin Hood", None, None,
      Some(false), Some("2017-11-01T12:00:00.000Z"), Some("honorable.sheriff"), Some("The Honorable Sheriff of Nottingham"), None)))
    val expectedMetadata = baseOpenMetadata.copy(domain = fontanaDomain.cname, approvals = approvals)
    testContextMetaData(vmDomain.cname, fontanaDomain.cname, "d0-v0", expectedMetadata)
  }

  test("visibility info for a fontana domain with an RA context") {
    val approvals = Some(Vector(FlattenedApproval("publicize", "approved", "2017-10-31T12:00:00.000Z", "robin-hood", "Robin Hood", None, None,
      Some(false), Some("2017-11-01T12:00:00.000Z"), Some("honorable.sheriff"), Some("The Honorable Sheriff of Nottingham"), None)))
    val expectedMetadata = baseOpenMetadata.copy(domain = fontanaDomain.cname, approvals = approvals)
    testContextMetaData(raDomain.cname, fontanaDomain.cname, "d0-v0", expectedMetadata)
  }

  test("visibility info for a fontana domain with a VM + RA context") {
    val approvals = Some(Vector(FlattenedApproval("publicize", "approved", "2017-10-31T12:00:00.000Z", "robin-hood", "Robin Hood", None, None,
      Some(false), Some("2017-11-01T12:00:00.000Z"), Some("honorable.sheriff"), Some("The Honorable Sheriff of Nottingham"), None)))
    val expectedMetadata = baseOpenMetadata.copy(domain = fontanaDomain.cname, approvals = approvals)
    testContextMetaData(vmRaDomain.cname, fontanaDomain.cname, "d0-v0", expectedMetadata)
  }

  // the status of views from VM domains will vary based on whether the context has VM or RA
  test("visibility info for a VM domain with a fontana domain context") {
    val expectedMetadata = baseOpenMetadata.copy(domain = vmDomain.cname, isModerationApproved = Some(true), moderationStatus = Some("approved"))
    testContextMetaData(fontanaDomain.cname, vmDomain.cname, "d1-v0", expectedMetadata)
  }

  test("visibility info for a VM domain with a VM context") {
    val expectedMetadata = baseOpenMetadata.copy(domain = vmDomain.cname, isModerationApproved = Some(true), moderationStatus = Some("approved"),
      isModerationApprovedOnContext = Some(true), isRoutingApprovedOnContext = Some(true))
    testContextMetaData(vmRaDomain.cname, vmDomain.cname, "d1-v0", expectedMetadata)
  }

  test("visibility info for a VM domain with an RA context") {
    val expectedMetadata = baseOpenMetadata.copy(domain = vmDomain.cname, isModerationApproved = Some(true), moderationStatus = Some("approved"),
      isRoutingApprovedOnContext = Some(true))
    testContextMetaData(raDomain.cname, vmDomain.cname, "d1-v0", expectedMetadata)
  }

  // the status of views from RA domains will vary based on whether the context has VM or RA
  test("visibility info for an RA domain with a fontana domain context") {
    val expectedMetadata = baseOpenMetadata.copy(domain = raDomain.cname, isRoutingApproved = Some(true), routingStatus = Some("approved"))
    testContextMetaData(fontanaDomain.cname, raDomain.cname, "d2-v2", expectedMetadata)
  }

  test("visibility info for an RA domain with a VM domain context") {
    val expectedMetadata = baseOpenMetadata.copy(domain = raDomain.cname, isRoutingApproved = Some(true), routingStatus = Some("approved"),
      isModerationApprovedOnContext = Some(true))
    testContextMetaData(vmDomain.cname, raDomain.cname, "d2-v2", expectedMetadata)
  }

  test("visibility info for an RA domain with an RA domain context") {
    val expectedMetadata = baseOpenMetadata.copy(domain = raDomain.cname, isRoutingApproved = Some(true), routingStatus = Some("approved"),
      isRoutingApprovedOnContext = Some(true), isModerationApprovedOnContext = Some(true))
    testContextMetaData(vmRaDomain.cname, raDomain.cname, "d2-v2", expectedMetadata)
  }

  // the status of views from VM + RA domains will vary based on whether the context has VM or RA
  test("visibility info for a VM + RA domain with a fontana domain context") {
    val expectedMetadata = baseOpenMetadata.copy(domain = vmRaDomain.cname, isModerationApproved = Some(true), isRoutingApproved = Some(true),
      moderationStatus = Some("approved"), routingStatus = Some("approved"))
    testContextMetaData(fontanaDomain.cname, vmRaDomain.cname, "d3-v3", expectedMetadata)
  }

  test("visibility info for a VM + RA domain with a VM domain context") {
    val expectedMetadata = baseOpenMetadata.copy(domain = vmRaDomain.cname, isModerationApproved = Some(true), isRoutingApproved = Some(true),
      moderationStatus = Some("approved"), routingStatus = Some("approved"), isModerationApprovedOnContext = Some(true))
    testContextMetaData(vmDomain.cname, vmRaDomain.cname, "d3-v3", expectedMetadata)
  }

  test("visibility info for a VM + RA domain with an RA domain context") {
    val expectedMetadata = baseOpenMetadata.copy(domain = vmRaDomain.cname, isModerationApproved = Some(true), isRoutingApproved = Some(true),
      moderationStatus = Some("approved"), routingStatus = Some("approved"), isRoutingApprovedOnContext = Some(true))
    testContextMetaData(raDomain.cname, vmRaDomain.cname, "d3-v3", expectedMetadata)
  }

  test("search response without a searchContext should have the correct set of documents") {
    val customerDomainIds = domains.filter(d => d.isCustomerDomain).map(_.id)
    val anonymousCustomerDocs = anonymouslyViewableDocs.filter(d => customerDomainIds.contains(d.socrataId.domainId))
    val expectedFxfs = anonymousCustomerDocs.map(_.socrataId.datasetId)
    val (_, res, _, _) = service.doSearch(Map.empty, AuthParams(), None, None)
    val actualFxfs = fxfs(res)
    actualFxfs should contain theSameElementsAs expectedFxfs
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
    val unmoderatedDomains = domains.filter(!_.moderationEnabled).map(_.cname)
    val params = Map("search_context" -> Seq(domains(0).cname), "domains" -> unmoderatedDomains)
    val (_, results, _, _) = service.doSearch(params, AuthParams(), None, None)
    val (_, resultsWithRedundantParams, _, _) = service.doSearch(params ++ browseParams, AuthParams(), None, None)

    val actualFxfs = fxfs(resultsWithRedundantParams)
    val expectedFxfs = fxfs(results)
    actualFxfs should contain theSameElementsAs (expectedFxfs)
  }

  test("adding on the set of 'browse' params should not change the set of anonymously viewable results when the search_context and domains are moderated") {
    val moderatedDomains = domains.filter(_.moderationEnabled).map(_.cname)
    val params = Map("search_context" -> Seq(domains(1).cname), "domains" -> moderatedDomains)
    val (_, results, _, _) = service.doSearch(params, AuthParams(), None, None)
    val (_, resultsWithRedundantParams, _, _) = service.doSearch(params ++ browseParams, AuthParams(), None, None)

    val actualFxfs = fxfs(resultsWithRedundantParams)
    val expectedFxfs = fxfs(results)
    actualFxfs should contain theSameElementsAs (expectedFxfs)
  }

  test("adding on the set of 'browse' params should not change the set of anonymously viewable results when the search_context is unmoderated, and the domains are moderated") {
    val moderatedDomains = domains.filter(_.moderationEnabled).map(_.cname)
    val params = Map("search_context" -> Seq(domains(0).cname), "domains" -> moderatedDomains)
    val (_, results, _, _) = service.doSearch(params, AuthParams(), None, None)
    val (_, resultsWithRedundantParams, _, _) = service.doSearch(params ++ browseParams, AuthParams(), None, None)

    val actualFxfs = fxfs(resultsWithRedundantParams)
    val expectedFxfs = fxfs(results)
    actualFxfs should contain theSameElementsAs (expectedFxfs)
  }

  test("adding on the set of 'browse' params should not change the set of anonymously viewable results when the search_context is moderated, and the domains are unmoderated") {
    val unmoderatedDomains = domains.filter(!_.moderationEnabled).map(_.cname)
    val params = Map("search_context" -> Seq(domains(1).cname), "domains" -> unmoderatedDomains)
    val (_, results, _, _) = service.doSearch(params, AuthParams(), None, None)
    val (_, resultsWithRedundantParams, _, _) = service.doSearch(params ++ browseParams, AuthParams(), None, None)

    val actualFxfs = fxfs(resultsWithRedundantParams)
    val expectedFxfs = fxfs(results)
    actualFxfs should contain theSameElementsAs (expectedFxfs)
  }

  //////////////////////////////////////////////////
  // Boosts
  //////////////////////////////////////////////////

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
      r.score should be(Some(0.0))
    }
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
    val resultTypes = results.results.map(_.resource.datatype)
    val topResultType = resultTypes.headOption
    topResultType should be(Some("story"))
  }

  test("giving a datatype a boost of <<1 should demote assets of that type to the bottom") {
    val params = Map("boostStories" -> Seq(".0000001"))
    val (_, results, _, _) = service.doSearch(params, AuthParams(), None, None)
    val resultTypes = results.results.map(_.resource.datatype)
    val lastResultType = resultTypes.last
    lastResultType should be("story")
  }

  //////////////////////////////////////////////////
  // Visibility
  //////////////////////////////////////////////////

  test("searching across all domains when no user is given returns anonymously viewable views on unlocked domains") {
    val res = service.doSearch(allDomainsParams, AuthParams(), None, None)
    fxfs(res._2) should contain theSameElementsAs anonymouslyViewableDocIds
  }

  test("private documents should always be hidden") {
    val (_, res, _, _) = service.doSearch(Map(
      Params.domains -> domains.map(_.cname).mkString(","),
      Params.public -> "false"
    ).mapValues(Seq(_)), AuthParams(), None, None)
    val actualFxfs = fxfs(res)
    actualFxfs should contain theSameElementsAs Set.empty
  }

  test("unpublished documents should always be hidden") {
    val (_, res, _, _) = service.doSearch(Map(
      Params.domains -> domains.map(_.cname).mkString(","),
      Params.published -> "false"
    ).mapValues(Seq(_)), AuthParams(), None, None)
    val actualFxfs = fxfs(res)
    actualFxfs should contain theSameElementsAs Set.empty
  }

  test("hidden documents should always be hidden") {
    val (_, res, _, _) = service.doSearch(Map(
      Params.domains -> domains.map(_.cname).mkString(","),
      Params.explicitlyHidden -> "true"
    ).mapValues(Seq(_)), AuthParams(), None, None)
    val actualFxfs = fxfs(res)
    actualFxfs should contain theSameElementsAs Set.empty
  }

  test("rejected documents should always be hidden") {
    val (_, res, _, _) = service.doSearch(Map(
      Params.domains -> domains.map(_.cname).mkString(","),
      Params.approvalStatus -> ApprovalStatus.rejected.status
    ).mapValues(Seq(_)), AuthParams(), None, None)
    val actualFxfs = fxfs(res)
    actualFxfs should contain theSameElementsAs Set.empty
  }

  test("pending documents should always be hidden") {
    val (_, res, _, _) = service.doSearch(Map(
      Params.domains -> domains.map(_.cname).mkString(","),
      Params.approvalStatus -> ApprovalStatus.pending.status
    ).mapValues(Seq(_)), AuthParams(), None, None)
    val actualFxfs = fxfs(res)
    actualFxfs should contain theSameElementsAs Set.empty
  }

  test("if a domain is locked and no auth is provided, nothing should come back") {
    val lockedDomain = domains(8).cname
    val params = Map(
      "domains" -> lockedDomain,
      "search_context" -> lockedDomain
    ).mapValues(Seq(_))
    val res = service.doSearch(params, AuthParams(), None, None)._2
    val actualFxfs = fxfs(res)
    // ensure nothing comes back
    actualFxfs should be('empty)

    // ensure something could have come back
    val docFrom8 = dom8Docs.headOption.get
    docFrom8.socrataId.domainId should be(domains(8).id)
    docFrom8.isPublic should be(true)
    docFrom8.isPublished should be(true)
    docFrom8.isApprovedByParentDomain should be(true)
  }

  test("if a user is provided, only anonymously viewabled datasets owned by that user should show up") {
    val params = Map("for_user" -> "robin-hood").mapValues(Seq(_))
    val expectedFxfs = anonymouslyViewableDocs.filter(d => d.owner.id == "robin-hood").map(_.socrataId.datasetId)
    val res = service.doSearch(params, AuthParams(), None, None)._2
    val actualFxfs = fxfs(res)
    actualFxfs should contain theSameElementsAs expectedFxfs
  }

  // TODO: consider searching for datasets by user screen name; using new custom analyzer
  ignore("if a user's name is queried, datasets with a matching owner:screen_name should show up") {
    val params = Map("q" -> "John").mapValues(Seq(_))
    val expectedFxfs = Set("d3-v4", "d2-v4")
    val res = service.doSearch(params, AuthParams(), None, None)._2
    val actualFxfs = fxfs(res)
    actualFxfs should contain theSameElementsAs expectedFxfs
  }

  test("if a parent dataset is provided, response should only include anonymously viewable views derived from that dataset") {
    val params = Map(Params.derivedFrom -> "d0-v0").mapValues(Seq(_))
    val expectedFxfs = anonymouslyViewableDocs.filter(d => d.socrataId.parentDatasetId.getOrElse(Set.empty).contains("d0-v0")).map(_.socrataId.datasetId)
    val res = service.doSearch(params, AuthParams(), None, None)._2
    val actualFxfs = fxfs(res)
    actualFxfs should contain theSameElementsAs expectedFxfs
  }

  test("searching with the 'q' param finds no items where q matches the private metadata") {
    val privateValue = "Cheetah Corp."
    val params = allDomainsParams ++ Map("q" -> privateValue).mapValues(Seq(_))
    val res = service.doSearch(params, AuthParams(), None, None)
    val actualFxfs = fxfs(res._2)
    actualFxfs should be('empty)

    // confirm there were documents that were excluded.
    val doc = anonymouslyViewableDocs.find(_.socrataId.datasetId == "d0-v2")
    doc.map(
      _.privateCustomerMetadataFlattened.exists(_.value == privateValue)
    ) should be(Some(true))
  }

  test("searching with a private metadata k/v pair param finds no items if the user doesn't own/share the doc") {
    val privateKey = "Secret domain 0 cat organization"
    val privateValue = "Cheetah Corp."
    val params = allDomainsParams ++ Map(privateKey -> Seq(privateValue))
    val res = service.doSearch(params, AuthParams(), None, None)
    val actualFxfs = fxfs(res._2)
    actualFxfs should be('empty)

    // confirm there were documents that were excluded.
    val doc = anonymouslyViewableDocs.find(_.socrataId.datasetId == "d0-v2")
    doc.map(
      _.privateCustomerMetadataFlattened.exists(m => m.value == privateValue && m.key == privateKey)
    ) should be(Some(true))
  }

  test("using 'visibility=open' filters nothing, since anonymous users already see 'open' views") {
    val dom = domains(0).cname
    val params = Map("search_context" -> Seq(dom), "domains" -> Seq(dom))
    val vizParams = Map("search_context" -> Seq(dom), "domains" -> Seq(dom), "visibility" -> Seq("open"))

    val (_, res, _, _) = service.doSearch(params, AuthParams(), None, None)
    val (_, resWithVizFilter, _, _) = service.doSearch(vizParams, AuthParams(), None, None)

    val actualFxfs = fxfs(resWithVizFilter)
    val expectedFxfs = fxfs(res)
    actualFxfs should contain theSameElementsAs (expectedFxfs)
  }

  test("using 'visibility=internal' filters everything, since anonymous users can only see 'open' views") {
    val dom = domains(0).cname
    val params = Map("search_context" -> Seq(dom), "domains" -> Seq(dom), "visibility" -> Seq("internal"))
    val (_, res, _, _) = service.doSearch(params, AuthParams(), None, None)
    val actualFxfs = fxfs(res)
    actualFxfs should be('empty)
  }

  //////////////////////////////////////////////////
  // Category searches and filters
  //////////////////////////////////////////////////

  test("categories filter should be case insensitive") {
    val paramsTitleCase = Map(
      "domains" -> "petercetera.net",
      "categories" -> "Personal"
    ).mapValues(Seq(_))

    val expectedFxfs = anonymousDomain0Docs.filter(_.animlAnnotations.map(_.categories)
      .map(_.exists(_.name.toLowerCase() == "personal")).get)
      .map(_.socrataId.datasetId)

    testCaseInsensitivity(paramsTitleCase, expectedFxfs)
  }

  test("custom domain categories filter should be case insensitive") {
    val paramsTitleCase = Map(
      "domains" -> "petercetera.net",
      "search_context" -> "petercetera.net",
      "categories" -> "Alpha"
    ).mapValues(Seq(_))

    val expectedFxfs = anonymousDomain0Docs.filter(_.customerCategory.exists(_.toLowerCase == "alpha"))
      .map(_.socrataId.datasetId)

    testCaseInsensitivity(paramsTitleCase, expectedFxfs)
  }

  test("categories filter should NOT include partial phrase matches") {
    val params = Map(
      "domains" -> "petercetera.net",
      "categories" -> "Personal"
    ).mapValues(Seq(_))

    val docsWithPartialMatch = anonymousDomain0Docs.filter(_.animlAnnotations.map(_.categories)
      .map(_.exists(_.name.toLowerCase().contains("personal"))).get)

    val expectedFxfs = anonymousDomain0Docs.filter(_.animlAnnotations.map(_.categories)
      .map(_.exists(_.name.toLowerCase() == "personal")).get)
      .map(_.socrataId.datasetId)

    docsWithPartialMatch.size should be > expectedFxfs.size

    testResultSet(params, Some(expectedFxfs))
  }

  test("custom domain categories filter should NOT include partial phrase matches") {
    val params = Map(
      "domains" -> "petercetera.net",
      "search_context" -> "petercetera.net",
      "categories" -> "Alpha"
    ).mapValues(Seq(_))

    val docsWithPartialMatch = anonymousDomain0Docs.filter(_.customerCategory.exists(_.contains("Alpha")))

    val expectedFxfs = anonymousDomain0Docs.filter(_.customerCategory.exists(_ == "Alpha")).map(_.socrataId.datasetId)

    docsWithPartialMatch.size should be > expectedFxfs.size

    testResultSet(params, Some(expectedFxfs))
  }

  test("categories filter should NOT include individual term matches") {
    val params = Map(
      "search_context" -> "petercetera.net",
      "domains" -> "petercetera.net",
      // the full category is "Alpha to Omega" and we used to include matches on any one of the terms e.g. Alpha
      "categories" -> "Alpha Beta Gaga"
    ).mapValues(Seq(_))

    testResultSet(params, None)
  }

  test("searching for a category should include case insensitive partial phrase matches") {
    val params = Map(
      "domains" -> "petercetera.net",
      "q" -> "PERSONAL"
    ).mapValues(Seq(_))

    val expectedFxfs = anonymousDomain0Docs.filter(_.animlAnnotations.map(_.categories)
      .map(_.exists(_.name.toLowerCase().contains("personal"))).get)
      .map(_.socrataId.datasetId)

    testResultSet(params, Some(expectedFxfs))
  }

  test("searching for a custom domain category should include case insensitive partial phrase matches") {
    val params = Map(
      "search_context" -> "petercetera.net",
      "domains" -> "petercetera.net",
      "q" -> "ALPHA"
    ).mapValues(Seq(_))

    val expectedFxfs = anonymousDomain0Docs.filter(_.customerCategory.exists(_.toLowerCase.contains("alpha")))
      .map(_.socrataId.datasetId)

    testResultSet(params, Some(expectedFxfs))
  }

  //////////////////////////////////////////////////
  // Tag searches and filters
  //////////////////////////////////////////////////

  test("tags filter should be case insensitive") {
    val paramsTitleCase = Map(
      "domains" -> "petercetera.net",
      "tags" -> "Happy"
    ).mapValues(Seq(_))

    val expectedFxfs = anonymousDomain0Docs.filter(_.animlAnnotations.map(_.tags)
      .map(_.exists(_.name.toLowerCase() == "happy")).get)
      .map(_.socrataId.datasetId)

    testCaseInsensitivity(paramsTitleCase, expectedFxfs)
  }

  test("custom domain tags filter should be case insensitive") {
    val paramsTitleCase = Map(
      "domains" -> "petercetera.net",
      "search_context" -> "petercetera.net",
      "tags" -> "1-One"
    ).mapValues(Seq(_))

    val expectedFxfs = anonymousDomain0Docs.filter(_.customerTags.map(_.toLowerCase()).contains("1-one"))
      .map(_.socrataId.datasetId)

    testCaseInsensitivity(paramsTitleCase, expectedFxfs)
  }

  test("tags filter should NOT include partial phrase matches") {
    val params = Map(
      "domains" -> "petercetera.net",
      "tags" -> "Happy"
    ).mapValues(Seq(_))

    val docsWithPartialMatch = anonymousDomain0Docs.filter(_.animlAnnotations.map(_.tags)
      .map(_.exists(_.name.toLowerCase().contains("happy"))).get)

    val expectedFxfs = anonymousDomain0Docs.filter(_.animlAnnotations.map(_.tags)
      .map(_.exists(_.name.toLowerCase() == "happy")).get)
      .map(_.socrataId.datasetId)

    docsWithPartialMatch.size should be > expectedFxfs.size

    testResultSet(params, Some(expectedFxfs))
  }

  test("custom domain tags filter should NOT include partial phrase matches") {
    val params = Map(
      "domains" -> "petercetera.net",
      "search_context" -> "petercetera.net",
      "tags" -> "1-one"
    ).mapValues(Seq(_))

    val docsWithPartialMatch = anonymousDomain0Docs.filter(_.customerTags.exists(_.contains("1-one")))

    val expectedFxfs = anonymousDomain0Docs.filter(_.customerTags.exists(_ == "1-one")).map(_.socrataId.datasetId)

    docsWithPartialMatch.size should be > expectedFxfs.size

    testResultSet(params, Some(expectedFxfs))
  }

  test("tags filter should NOT include individual term matches") {
    val params = Map(
      "search_context" -> "petercetera.net",
      "domains" -> "petercetera.net",
      // the full category is "Alpha to Omega" and we used to include matches on any one of the terms e.g. Alpha
      "tags" -> "Happy Days Is My Favorite"
    ).mapValues(Seq(_))

    testResultSet(params, None)
  }

  test("searching for a tag should include case insensitive partial phrase matches") {
    val params = Map(
      "domains" -> "petercetera.net",
      "q" -> "HAPPY"
    ).mapValues(Seq(_))

    val expectedFxfs = anonymousDomain0Docs.filter(_.animlAnnotations.map(_.tags)
      .map(_.exists(_.name.toLowerCase().contains("happy"))).get)
      .map(_.socrataId.datasetId)

    testResultSet(params, Some(expectedFxfs))
  }

  test("searching for a custom domain tag should include case insensitive partial phrase matches") {
    val params = Map(
      "search_context" -> "petercetera.net",
      "domains" -> "petercetera.net",
      "q" -> "1-one"
    ).mapValues(Seq(_))

    val expectedFxfs = anonymousDomain0Docs.filter(_.customerTags.exists(_.contains("1-one"))).map(_.socrataId.datasetId)

    testResultSet(params, Some(expectedFxfs))
  }

  //////////////////////////////////////////////////
  // Sorting
  //////////////////////////////////////////////////

  test("sorting by name works") {
    val params = Map("order" -> Seq("name"))
    val (_, results, _, _) = service.doSearch(params, AuthParams(), None, None)
    results.results.head.resource.name should be("")
    results.results.last.resource.name should be("Three")
  }

  test("sorting by name DESC works") {
    val params = Map("order" -> Seq("name DESC"), "domains" -> Seq("robert.demo.socrata.com"))
    val (_, results, _, _) = service.doSearch(params, AuthParams(), None, None)
    results.results.head.resource.name should be("My Latest and Greatest Dataset")
    results.results.last.resource.name should be("Alfa")
  }

  test("sorting by name ignores non-alphanumeric characters") {
    val params = Map("order" -> Seq("name"), "domains" -> Seq("robert.demo.socrata.com"))
    val (_, results, _, _) = service.doSearch(params, AuthParams(), None, None)
    results.results.head.resource.name should be("Alfa")
    results.results.last.resource.name should be("My Latest and Greatest Dataset")
  }

  test("sorting by name ignores case") {
    val params = Map("order" -> Seq("name"), "ids" -> Seq("d9-v2", "d9-v4"))
    val (_, results, _, _) = service.doSearch(params, AuthParams(), None, None)
    results.results.head.resource.name should be("'bravo")
    results.results.last.resource.name should be("Charlie")
  }

  //////////////////////////////////////////////////
  // Attribution
  //////////////////////////////////////////////////

  test("filtering by attribution works") {
    val params = Map("attribution" -> Seq("The Merry Men"))
    val (_, results, _, _) = service.doSearch(params, AuthParams(), None, None)
    val expectedFxfs = Set("d0-v7")
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
    val expectedFxfs = Set("d0-v7")
    val actualFxfs = fxfs(results)
    actualFxfs should contain theSameElementsAs expectedFxfs
  }

  test("attribution is included in the resulting resource") {
    val params = Map("q" -> Seq("merry men"))
    val (_, results, _, _) = service.doSearch(params, AuthParams(), None, None)
    results.results(0).resource.attribution should be(Some("The Merry Men"))
  }

  //////////////////////////////////////////////////
  // Provenance
  //////////////////////////////////////////////////

  test("filtering by provenance works and the resulting resource has a 'provenance' field") {
    val params = Map("provenance" -> Seq("official"))
    val (_, results, _, _) = service.doSearch(params, AuthParams(), None, None)
    results.results(0).resource.provenance should be(Some("official"))
  }

  //////////////////////////////////////////////////
  // License
  //////////////////////////////////////////////////

  test("filtering by license works and the resulting metadata has a 'license' field") {
    val params = Map("license" -> Seq("Academic Free License"))
    val (_, results, _, _) = service.doSearch(params, AuthParams(), None, None)
    results.results(0).metadata.license should be(Some("Academic Free License"))
  }

  test("filtering by license should be exact") {
    val params = Map("license" -> Seq("Free License"))
    val (_, results, _, _) = service.doSearch(params, AuthParams(), None, None)
    results.results should be('empty)
  }

  test("filtering by license should be case sensitive") {
    val params = Map("license" -> Seq("academic free license"))
    val (_, results, _, _) = service.doSearch(params, AuthParams(), None, None)
    results.results should be('empty)
  }

  test("a query that results in a dataset with a license should return metadata with a 'license' field") {
    val params = Map("q" -> Seq("Five"))
    val (_, results, _, _) = service.doSearch(params, AuthParams(), None, None)
    results.results(0).metadata.license should be(Some("Academic Free License"))
  }

  //////////////////////////////////////////////////
  // Preview images
  //////////////////////////////////////////////////

  test("preview_image_url should be included in the search result when available") {
    val params = Map("ids" -> Seq("d0-v7"))
    val (_, results, _, _) = service.doSearch(params, AuthParams(), None, None)
    val firstPreviewImageUrl = results.results(0).previewImageUrl
    firstPreviewImageUrl should be(Some("https://petercetera.net/views/d0-v7/files/123456789"))
  }

  test("preview_image_url should be None in the search result when not available") {
    val params = Map("ids" -> Seq("d0-v0"))
    val (_, results, _, _) = service.doSearch(params, AuthParams(), None, None)
    val firstPreviewImageUrl = results.results(0).previewImageUrl
    firstPreviewImageUrl should be(None)
  }

  //////////////////////////////////////////////////
  // Params
  //////////////////////////////////////////////////

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

  test("filtering by column names works") {
    val params = Map("column_names[]" -> Seq("first_name"))
    val (_, results, _, _) = service.doSearch(params, AuthParams(), None, None)
    results.results(0).resource.columnsName should be(List("first_name"))
  }

  test("searching for column names works via the 'q' param works") {
    val params = Map("q" -> Seq("first_name"))
    val (_, results, _, _) = service.doSearch(params, AuthParams(), None, None)
    results.results(0).resource.columnsName should be(List("first_name"))
  }

  test("searching by column names is case insensitive") {
    val params = Map("column_names[]" -> Seq("FIRST_NAME"))
    val (_, results, _, _) = service.doSearch(params, AuthParams(), None, None)
    results.results(0).resource.columnsName should be(List("first_name"))
  }
}
