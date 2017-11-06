package com.socrata.cetera.services

import com.rojoma.json.v3.ast.JValue
import org.scalatest.{BeforeAndAfterAll, BeforeAndAfterEach, FunSuiteLike, Matchers}

import com.socrata.cetera.auth.AuthParams
import com.socrata.cetera.types.{ApprovalStatus, DomainSet}
import com.socrata.cetera.{response => _, _}

class SearchServiceSpecForSuperAdmins
  extends FunSuiteLike
    with Matchers
    with TestESData
    with BeforeAndAfterAll
    with BeforeAndAfterEach {

  override protected def beforeAll(): Unit = bootstrapData()

  override def beforeEach(): Unit = mockServer.reset()

  override protected def afterAll(): Unit = {
    removeBootstrapData()
    client.close()
    httpClient.close()
  }

  test("searching across all domains with a super admin shows everything from every domain") {
    val expectedFxfs = fxfs(docs)

    val host = "annabelle.island.net"
    prepareAuthenticatedUser(cookie, host, superAdminBody)
    val res = service.doSearch(allDomainsParams, AuthParams(cookie=Some(cookie)), Some(host), None)
    val actualFxfs = fxfs(res._2)

    actualFxfs should contain theSameElementsAs expectedFxfs
  }

  test("searching on a locked domains with a super admin shows data") {
    val lockedDomain = domains(8).domainCname
    val params = Map(
      "domains" -> lockedDomain,
      "search_context" -> lockedDomain
    ).mapValues(Seq(_))
    val expectedFxfs = fxfs(docs.filter(d => d.socrataId.domainId == 8))
    prepareAuthenticatedUser(cookie, lockedDomain, superAdminBody)
    val adminRes = service.doSearch(params, AuthParams(cookie = Some(cookie)), Some(lockedDomain), None)._2
    fxfs(adminRes) should contain theSameElementsAs expectedFxfs
  }

  test("searching across all domains as a superadmin with public=true, should find all public views") {
    val expectedFxfs = fxfs(docs.filter(d => d.isPublic))

    val host = "annabelle.island.net"
    prepareAuthenticatedUser(cookie, host, superAdminBody)
    val params = allDomainsParams ++ Map("public" -> Seq("true"))
    val res = service.doSearch(params, AuthParams(cookie=Some(cookie)), Some(host), None)
    val actualFxfs = fxfs(res._2)

    actualFxfs should contain theSameElementsAs expectedFxfs
  }

  test("searching across all domains as a superadmin with public=false, should find all private views") {
    val expectedFxfs = fxfs(docs.filter(d => !d.isPublic))

    val host = "annabelle.island.net"
    prepareAuthenticatedUser(cookie, host, superAdminBody)
    val params = allDomainsParams ++ Map("public" -> Seq("false"))
    val res = service.doSearch(params, AuthParams(cookie=Some(cookie)), Some(host), None)
    val actualFxfs = fxfs(res._2)

    actualFxfs should contain theSameElementsAs expectedFxfs
  }

  test("searching across all domains as a superadmin with published=true, should find all published views") {
    val expectedFxfs = fxfs(docs.filter(d => d.isPublished))

    val host = "annabelle.island.net"
    prepareAuthenticatedUser(cookie, host, superAdminBody)
    val params = allDomainsParams ++ Map("published" -> Seq("true"))
    val res = service.doSearch(params, AuthParams(cookie=Some(cookie)), Some(host), None)
    val actualFxfs = fxfs(res._2)

    actualFxfs should contain theSameElementsAs expectedFxfs
  }

  test("searching across all domains as a superadmin with published=false, should find all unpublished views") {
    val expectedFxfs = fxfs(docs.filter(d => !d.isPublished))

    val host = "annabelle.island.net"
    prepareAuthenticatedUser(cookie, host, superAdminBody)
    val params = allDomainsParams ++ Map("published" -> Seq("false"))
    val res = service.doSearch(params, AuthParams(cookie=Some(cookie)), Some(host), None)
    val actualFxfs = fxfs(res._2)

    actualFxfs should contain theSameElementsAs expectedFxfs
  }

  test("searching across all domains as a superadmin with derived=true, should find all derived views") {
    val expectedFxfs = fxfs(docs.filter(d => !d.isDefaultView))

    val host = "annabelle.island.net"
    prepareAuthenticatedUser(cookie, host, superAdminBody)
    val params = allDomainsParams ++ Map("derived" -> Seq("true"))
    val res = service.doSearch(params, AuthParams(cookie=Some(cookie)), Some(host), None)
    val actualFxfs = fxfs(res._2)

    actualFxfs should contain theSameElementsAs expectedFxfs
  }

  test("searching across all domains as a superadmin with derived=false, should find all default views") {
    val expectedFxfs = fxfs(docs.filter(d => d.isDefaultView))

    val host = "annabelle.island.net"
    prepareAuthenticatedUser(cookie, host, superAdminBody)
    val params = allDomainsParams ++ Map("derived" -> Seq("false"))
    val res = service.doSearch(params, AuthParams(cookie=Some(cookie)), Some(host), None)
    val actualFxfs = fxfs(res._2)

    actualFxfs should contain theSameElementsAs expectedFxfs
  }

  test("searching across all domains as a superadmin with explicity_hidden=true, should find all hidden views") {
    val expectedFxfs = fxfs(docs.filter(d => d.isHiddenFromCatalog))

    val host = "annabelle.island.net"
    prepareAuthenticatedUser(cookie, host, superAdminBody)
    val params = allDomainsParams ++ Map("explicitly_hidden" -> Seq("true"))
    val res = service.doSearch(params, AuthParams(cookie=Some(cookie)), Some(host), None)
    val actualFxfs = fxfs(res._2)

    actualFxfs should contain theSameElementsAs expectedFxfs
  }

  test("searching across all domains as a superadmin with explicitly_hidden=false, should find all not-hidden views") {
    val expectedFxfs = fxfs(docs.filter(d => !d.isHiddenFromCatalog))

    val host = "annabelle.island.net"
    prepareAuthenticatedUser(cookie, host, superAdminBody)
    val params = allDomainsParams ++ Map("explicitly_hidden" -> Seq("false"))
    val res = service.doSearch(params, AuthParams(cookie=Some(cookie)), Some(host), None)
    val actualFxfs = fxfs(res._2)

    actualFxfs should contain theSameElementsAs expectedFxfs
  }

  test("searching across all domains as a superadmin with approval_status=approved and no context should find all approved results") {
    val allPossibleResults = getAllPossibleResults()
    val expectedFxfs = allPossibleResults.filter(isApproved).map(fxfs)

    val host = "annabelle.island.net"
    prepareAuthenticatedUser(cookie, host, superAdminBody)
    val params = allDomainsParams ++ Map("approval_status" -> Seq("approved"))
    val res = service.doSearch(params, AuthParams(cookie=Some(cookie)), Some(host), None)
    val actualFxfs = fxfs(res._2)
    actualFxfs should contain theSameElementsAs expectedFxfs
  }

  test("searching across all domains as a superadmin with approval_status=approved and a basic context should find all approved results") {
    val basicDomain = domains(0).domainCname
    val allPossibleResults = getAllPossibleResults()
    val expectedFxfs = allPossibleResults.filter(isApproved).map(fxfs)

    prepareAuthenticatedUser(cookie, basicDomain, superAdminBody)
    val params = allDomainsParams ++ Map("approval_status" -> Seq("approved"), "search_context" -> Seq(basicDomain))
    val res = service.doSearch(params, AuthParams(cookie=Some(cookie)), Some(basicDomain), None)
    val actualFxfs = fxfs(res._2)
    actualFxfs should contain theSameElementsAs expectedFxfs
  }

  test("searching across all domains as a superadmin with approval_status=approved and a moderated context should find only approved default views from unmoderated domains and approved views from moderated domains") {
    // this scenario is a little funny. the moderated search context says 'throw out all derived views from unmoderated domains'
    val allPossibleResults = getAllPossibleResults()
    val approvedFxfs = allPossibleResults.filter(isApproved).map(fxfs)
    val unmoderatedDomainIds = domains.filter(!_.moderationEnabled).map(_.domainId)
    val derivedViewsFromUnmoderatedDomains = fxfs(docs.filter(d => !d.isDefaultView && unmoderatedDomainIds.contains(d.socrataId.domainId)))
    val expectedFxfs = approvedFxfs.toSet -- derivedViewsFromUnmoderatedDomains.toSet

    val host = domains(1).domainCname
    prepareAuthenticatedUser(cookie, host, superAdminBody)
    val params = allDomainsParams ++ Map("approval_status" -> Seq("approved"), "search_context" -> Seq(host))
    val res = service.doSearch(params, AuthParams(cookie=Some(cookie)), Some(host), None)
    val actualFxfs = fxfs(res._2)
    actualFxfs should contain theSameElementsAs expectedFxfs
  }

  test("searching across all domains as a superadmin with approval_status=approved and an RA-enabled context should find only views approved by the contexts RA process") {
    val host = domains(2)
    val allPossibleResults = getAllPossibleResults()
    val approvedFxfs = allPossibleResults.filter(isApproved).map(fxfs)
    val expectedFxfs = fxfs(docs.filter(d => approvedFxfs.contains(d.socrataId.datasetId) && d.isRaApproved(host.domainId)))

    prepareAuthenticatedUser(cookie, host.domainCname, superAdminBody)
    val params = allDomainsParams ++ Map("approval_status" -> Seq("approved"), "search_context" -> Seq(host.domainCname))
    val res = service.doSearch(params, AuthParams(cookie=Some(cookie)), Some(host.domainCname), None)
    val actualFxfs = fxfs(res._2)
    actualFxfs should contain theSameElementsAs expectedFxfs
  }

  test("searching across all domains as a superadmin with approval_status=approved and a moderated & RA-enabled context should find only views approved by the context's VM and RA processes") {
    val host = domains(3)
    val allPossibleResults = getAllPossibleResults()
    val approvedFxfs = allPossibleResults.filter(isApproved).map(fxfs)
    val unmoderatedDomainIds = domains.filter(!_.moderationEnabled).map(_.domainId)
    val derivedViewsFromUnmoderatedDomains = fxfs(docs.filter(d => !d.isDefaultView && unmoderatedDomainIds.contains(d.socrataId.domainId)))
    val raApproved = fxfs(docs.filter(d => approvedFxfs.contains(d.socrataId.datasetId) && d.isRaApproved(host.domainId)))
    val expectedFxfs = raApproved.toSet -- derivedViewsFromUnmoderatedDomains.toSet

    prepareAuthenticatedUser(cookie, host.domainCname, superAdminBody)
    val params = allDomainsParams ++ Map("approval_status" -> Seq("approved"), "search_context" -> Seq(host.domainCname))
    val res = service.doSearch(params, AuthParams(cookie=Some(cookie)), Some(host.domainCname), None)
    val actualFxfs = fxfs(res._2)
    actualFxfs should contain theSameElementsAs expectedFxfs

    // d2-v4 is approved on 3, but is sadly a derived view and 3's moderated context won't have that.
    // this verifies that d2-v4 could've showed up, but didn't  (and ensures no one accidentally removes this test case)
    approvedFxfs should contain("d2-v4")
    derivedViewsFromUnmoderatedDomains should contain("d2-v4")
  }

  test("searching across all domains as a superadmin with approval_status=pending and no context should find all pending results") {
    val moderatedDomainIds = domains.filter(_.moderationEnabled).map(_.domainId)
    val raDomainIds = domains.filter(_.routingApprovalEnabled).map(_.domainId)
    val canbeModeratedDocs = docs.filter(d => moderatedDomainIds.contains(d.socrataId.domainId))
    val canbeRaDocs = docs.filter(d => raDomainIds.contains(d.socrataId.domainId))
    val vmPendingFxfs = fxfs(canbeModeratedDocs.filter(_.isVmPending))
    val raPendingFxfs = fxfs(canbeRaDocs.filter(d => d.isRaPending(d.socrataId.domainId.toInt)))
    val expectedFxfs = (vmPendingFxfs ++ raPendingFxfs).toSet

    val host = "annabelle.island.net"
    prepareAuthenticatedUser(cookie, host, superAdminBody)
    val params = allDomainsParams ++ Map("approval_status" -> Seq("pending"))
    val res = service.doSearch(params, AuthParams(cookie=Some(cookie)), Some(host), None)
    val actualFxfs = fxfs(res._2)
    actualFxfs should contain theSameElementsAs expectedFxfs
  }

  test("searching across all domains as a superadmin with approval_status=pending and a basic context should find all pending results") {
    val basicDomain = domains(0).domainCname
    val moderatedDomainIds = domains.filter(_.moderationEnabled).map(_.domainId)
    val raDomainIds = domains.filter(_.routingApprovalEnabled).map(_.domainId)
    val canbeModeratedDocs = docs.filter(d => moderatedDomainIds.contains(d.socrataId.domainId))
    val canbeRaDocs = docs.filter(d => raDomainIds.contains(d.socrataId.domainId))
    val vmPendingFxfs = fxfs(canbeModeratedDocs.filter(_.isVmPending))
    val raPendingFxfs = fxfs(canbeRaDocs.filter(d => d.isRaPending(d.socrataId.domainId.toInt)))
    val expectedFxfs = (vmPendingFxfs ++ raPendingFxfs).toSet

    prepareAuthenticatedUser(cookie, basicDomain, superAdminBody)
    val params = allDomainsParams ++ Map("approval_status" -> Seq("pending"), "search_context" -> Seq(basicDomain))
    val res = service.doSearch(params, AuthParams(cookie=Some(cookie)), Some(basicDomain), None)
    val actualFxfs = fxfs(res._2)
    actualFxfs should contain theSameElementsAs expectedFxfs
  }

  test("searching across all domains as a superadmin with approval_status=pending and a moderated context should find all pending results from moderated domains") {
    val moderatedDomainIds = domains.filter(_.moderationEnabled).map(_.domainId)
    val fromModeratedDomainDocs = docs.filter(d => moderatedDomainIds.contains(d.socrataId.domainId))
    val expectedFxfs = fxfs(fromModeratedDomainDocs.filter(_.isVmPending))

    val host = domains(1).domainCname
    prepareAuthenticatedUser(cookie, host, superAdminBody)
    val params = allDomainsParams ++ Map("approval_status" -> Seq("pending"), "search_context" -> Seq(host))
    val res = service.doSearch(params, AuthParams(cookie=Some(cookie)), Some(host), None)
    val actualFxfs = fxfs(res._2)
    actualFxfs should contain theSameElementsAs expectedFxfs
  }

  test("searching across all domains as a superadmin with approval_status=pending and an RA-enabled context should find " +
    "only pending views that have been in the contexts RA queue") {
    val host = domains(2)
    val docsInContextsQueue = docs.filter(_.isInRaQueue(host.domainId))
    val expectedFxfs = fxfs(docsInContextsQueue.filter(d =>
      d.isPendingOnParentDomain || d.isRaPending(2) ||  d.moderationStatus == ApprovalStatus.pending))

    prepareAuthenticatedUser(cookie, host.domainCname, superAdminBody)
    val params = allDomainsParams ++ Map("approval_status" -> Seq("pending"), "search_context" -> Seq(host.domainCname))
    val res = service.doSearch(params, AuthParams(cookie=Some(cookie)), Some(host.domainCname), None)
    val actualFxfs = fxfs(res._2)
    actualFxfs should contain theSameElementsAs expectedFxfs
  }

  test("searching across all domains as a superadmin with approval_status=pending and a moderated & RA-enabled context " +
    "should find only pending views that have been in the contexts RA queue and that aren't derived views from unmoderated domains") {
    val host = domains(3)
    val docsInContextsQueue = docs.filter(_.isInRaQueue(host.domainId))
    val unmoderatedDomainIds = domains.filter(!_.moderationEnabled).map(_.domainId)
    val derivedViewsFromUnmoderatedDomains = docs.filter(d => !d.isDefaultView && unmoderatedDomainIds.contains(d.socrataId.domainId))
    val possibleDocs = (docsInContextsQueue.toSet -- derivedViewsFromUnmoderatedDomains.toSet).toSeq
    val expectedFxfs = fxfs(possibleDocs.filter(d => d.isPendingOnParentDomain ||
      d.isRaPending(host.domainId) || d.isVmPending))

    prepareAuthenticatedUser(cookie, host.domainCname, superAdminBody)
    val params = allDomainsParams ++ Map("approval_status" -> Seq("pending"), "search_context" -> Seq(host.domainCname))
    val res = service.doSearch(params, AuthParams(cookie=Some(cookie)), Some(host.domainCname), None)
    val actualFxfs = fxfs(res._2)
    actualFxfs should contain theSameElementsAs expectedFxfs
  }

  test("searching across all domains as a superadmin with approval_status=rejected and no context should find all rejected results") {
    val moderatedDomainIds = domains.filter(_.moderationEnabled).map(_.domainId)
    val canbeModeratedDocs = docs.filter(d => moderatedDomainIds.contains(d.socrataId.domainId))
    val rejectedByVM = canbeModeratedDocs.filter(_.isVmRejected)
    val rejectedByRA = docs.filter(_.isRejectedByParentDomain)
    val expectedFxfs = fxfs(rejectedByVM ++ rejectedByRA)

    val host = "annabelle.island.net"
    prepareAuthenticatedUser(cookie, host, superAdminBody)
    val params = allDomainsParams ++ Map("approval_status" -> Seq("rejected"))
    val res = service.doSearch(params, AuthParams(cookie=Some(cookie)), Some(host), None)
    val actualFxfs = fxfs(res._2)
    actualFxfs should contain theSameElementsAs expectedFxfs
  }

  test("searching across all domains as a superadmin with approval_status=rejected and a basic context should find all rejected results") {
    val basicDomain = domains(0).domainCname
    val moderatedDomainIds = domains.filter(_.moderationEnabled).map(_.domainId)
    val canbeModeratedDocs = docs.filter(d => moderatedDomainIds.contains(d.socrataId.domainId))
    val rejectedByVM = canbeModeratedDocs.filter(_.isVmRejected)
    val rejectedByRA = docs.filter(_.isRejectedByParentDomain)
    val expectedFxfs = fxfs(rejectedByVM ++ rejectedByRA)

    prepareAuthenticatedUser(cookie, basicDomain, superAdminBody)
    val params = allDomainsParams ++ Map("approval_status" -> Seq("rejected"), "search_context" -> Seq(basicDomain))
    val res = service.doSearch(params, AuthParams(cookie=Some(cookie)), Some(basicDomain), None)
    val actualFxfs = fxfs(res._2)
    actualFxfs should contain theSameElementsAs expectedFxfs
  }

  test("searching across all domains as a superadmin with approval_status=rejected and a moderated context should find all rejected results") {
    // Note that rejected DL from unmoderated domains should not show here, b/c the moderated search context removes all
    // derived views from unmoderated domains
    val unmoderatedDomainIds = domains.filter(!_.moderationEnabled).map(_.domainId)
    val derivedViewsFromUnmoderatedDomains = docs.filter(d => !d.isDefaultView && unmoderatedDomainIds.contains(d.socrataId.domainId))
    val possibleDocs = (docs.toSet -- derivedViewsFromUnmoderatedDomains.toSet).toSeq
    val expectedFxfs = fxfs(possibleDocs.filter(d => d.isVmRejected || d.isRejectedByParentDomain))

    val host = domains(1).domainCname
    prepareAuthenticatedUser(cookie, host, superAdminBody)
    val params = allDomainsParams ++ Map("approval_status" -> Seq("rejected"), "search_context" -> Seq(host))
    val res = service.doSearch(params, AuthParams(cookie=Some(cookie)), Some(host), None)
    val actualFxfs = fxfs(res._2)
    actualFxfs should contain theSameElementsAs expectedFxfs
  }

  test("searching across all domains as a superadmin with approval_status=rejected and an RA-enabled context should find only rejected views that have been in the contexts RA queue") {
    val host = domains(2)
    val docsInContextsQueue = docs.filter(_.isInRaQueue(host.domainId))
    val expectedFxfs = fxfs(docsInContextsQueue.filter(d => d.isRejectedByParentDomain || d.isRaRejected(host.domainId) || d.isVmRejected))

    prepareAuthenticatedUser(cookie, host.domainCname, superAdminBody)
    val params = allDomainsParams ++ Map("approval_status" -> Seq("rejected"), "search_context" -> Seq(host.domainCname))
    val res = service.doSearch(params, AuthParams(cookie=Some(cookie)), Some(host.domainCname), None)
    val actualFxfs = fxfs(res._2)
    actualFxfs should contain theSameElementsAs expectedFxfs
  }

  test("searching across all domains as a superadmin with approval_status=rejected and a moderated & RA-enabled context " +
    "should find only rejected views that have been in the contexts RA queue and that aren't derived views from unmoderated domains") {
    val host = domains(3)
    val docsInContextsQueue = docs.filter(_.isInRaQueue(host.domainId))
    val unmoderatedDomainIds = domains.filter(!_.moderationEnabled).map(_.domainId)
    val derivedViewsFromUnmoderatedDomains = docs.filter(d => !d.isDefaultView && unmoderatedDomainIds.contains(d.socrataId.domainId))
    val possibleDocs = (docsInContextsQueue.toSet -- derivedViewsFromUnmoderatedDomains.toSet).toSeq
    val expectedFxfs = fxfs(possibleDocs.filter(d => d.isRejectedByParentDomain ||
      d.isRaRejected(host.domainId) || d.moderationStatus == ApprovalStatus.rejected))

    prepareAuthenticatedUser(cookie, host.domainCname, superAdminBody)
    val params = allDomainsParams ++ Map("approval_status" -> Seq("rejected"), "search_context" -> Seq(host.domainCname))
    val res = service.doSearch(params, AuthParams(cookie=Some(cookie)), Some(host.domainCname), None)
    val actualFxfs = fxfs(res._2)
    actualFxfs should contain theSameElementsAs expectedFxfs
  }

  test("searching with the 'q' param finds all items where q matches the private metadata") {
    val host = domains(0).domainCname
    val privateValue = "Cheetah Corp."
    val expectedFxfs = fxfs(docs.filter(d => d.privateCustomerMetadataFlattened.exists(m => m.value == privateValue)))
    prepareAuthenticatedUser(cookie, host, superAdminBody)
    val params = allDomainsParams ++ Map("q" -> Seq(privateValue))
    val res = service.doSearch(params, AuthParams(cookie=Some(cookie)), Some(host), None)
    val actualFxfs = fxfs(res._2)
    actualFxfs should contain theSameElementsAs expectedFxfs
  }

  test("searching with a private metadata k/v pair param finds all documents with that pair") {
    val host = domains(0).domainCname
    val privateKey = "Secret domain 0 cat organization"
    val privateValue = "Pumas Inc."
    val expectedFxfs = fxfs(docs.filter(d =>
      d.privateCustomerMetadataFlattened.exists(m => m.value == privateValue && m.key == privateKey)))
    prepareAuthenticatedUser(cookie, host, superAdminBody)
    val params = allDomainsParams ++ Map(privateKey -> Seq(privateValue))
    val res = service.doSearch(params, AuthParams(cookie=Some(cookie)), Some(host), None)
    val actualFxfs = fxfs(res._2)
    actualFxfs should contain theSameElementsAs expectedFxfs
  }

  test("searching with the 'shared_to' param works for super admins even if they are looking for data shared to others") {
    val host = domains(3).domainCname
    val expectedFxfs = fxfs(docs.filter(d => d.sharedTo.contains("lil-john")))
    prepareAuthenticatedUser(cookie, host, superAdminBody)
    val params = allDomainsParams ++ Map("shared_to" -> Seq("lil-john"))
    val res = service.doSearch(params, AuthParams(cookie=Some(cookie)), Some(host), None)
    val actualFxfs = fxfs(res._2)
    actualFxfs should contain theSameElementsAs expectedFxfs
  }
}
