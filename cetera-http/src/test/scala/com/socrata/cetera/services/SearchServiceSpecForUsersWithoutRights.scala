package com.socrata.cetera.services

import com.rojoma.json.v3.interpolation._
import org.scalatest.{BeforeAndAfterAll, BeforeAndAfterEach, FunSuiteLike, Matchers}

import com.socrata.cetera.auth.AuthParams
import com.socrata.cetera.types.{Document, Domain}
import com.socrata.cetera.{response => _, _}

class SearchServiceSpecForUsersWithoutRights
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

  val cookieMonsWithoutRole = authedUserBodyWithoutRoleOrRights()
  val cookieMonsWithRole = authedUserBodyWithRoleAndRights(List.empty)
  val robinHoodWithoutRole = authedUserBodyWithoutRoleOrRights("robin-hood")

  test("searching across all domains finds " +
    "a) anonymously visible views from their domain " +
    "b) anonymously visible views from unlocked domains " +
    "c) views they own/share") {
    val authenticatingDomain = domains(0)
    val ownedByCookieMonster = docs.collect{ case d: Document if d.owner.id == "cook-mons" => d.socrataId.datasetId }
    val sharedToCookieMonster = docs.collect{ case d: Document if d.sharedTo.contains("cook-mons") => d.socrataId.datasetId }
    val expectedFxfs = (ownedByCookieMonster ++ sharedToCookieMonster ++ anonymouslyViewableDocIds).distinct

    val host = authenticatingDomain.domainCname
    prepareAuthenticatedUser(cookie, host, cookieMonsWithoutRole)
    val res = service.doSearch(allDomainsParams, AuthParams(cookie = Some(cookie)), Some(host), None)
    fxfs(res._2) should contain theSameElementsAs expectedFxfs
  }

  test("searching on a locked domains shows data if the user has a role") {
    val lockedDomain = domains(8).domainCname
    val params = Map(
      "domains" -> lockedDomain,
      "search_context" -> lockedDomain
    ).mapValues(Seq(_))
    val expectedFxfs = fxfs(docs.filter(d => d.socrataId.domainId == 8))
    prepareAuthenticatedUser(cookie, lockedDomain, cookieMonsWithRole)
    val res = service.doSearch(params, AuthParams(cookie = Some(cookie)), Some(lockedDomain), None)._2
    fxfs(res) should contain theSameElementsAs expectedFxfs
  }

  test("searching on a locked domains if the user has no role, should show nothing") {
    val lockedDomain = domains(8).domainCname
    val params = Map(
      "domains" -> lockedDomain,
      "search_context" -> lockedDomain
    ).mapValues(Seq(_))

    prepareAuthenticatedUser(cookie, lockedDomain, cookieMonsWithoutRole)
    val res = service.doSearch(params, AuthParams(cookie = Some(cookie)), Some(lockedDomain), None)._2
    fxfs(res) should be('empty)
  }

  test("searching for assets owned by logged in user returns all assets the user owns, " +
    "regardless of private/public/approval status and regardless of what domain they come from") {
    // this has robin-hood looking up his own data.
    val authenticatingDomain = domains(0).domainCname
    val params = Map("for_user" -> Seq("robin-hood"))
    prepareAuthenticatedUser(cookie, authenticatingDomain, authedUserBodyWithoutRoleOrRights("robin-hood"))

    val docsOwnedByRobin = docs.filter(_.owner.id == "robin-hood")
    val ownedByRobinIds = fxfs(docsOwnedByRobin).toSet
    val res = service.doSearch(params, AuthParams(cookie = Some(cookie)), Some(authenticatingDomain), None)

    // confirm that robin owns view on domains other than 0
    docsOwnedByRobin.filter(_.socrataId.domainId !=0) shouldNot be('empty)
    fxfs(res._2) should contain theSameElementsAs ownedByRobinIds
  }

  test("searching for assets shared to a logged in user returns all assets the user shares, " +
    "regardless of private/public/approval status and regardless of what domain they come from") {
    // this has little john looking up data shared to him.
    val authenticatingDomain = domains(0).domainCname
    val name = "Little John"
    val params = Map("shared_to" -> Seq(name))
    prepareAuthenticatedUser(cookie, authenticatingDomain, authedUserBodyWithoutRoleOrRights(name))

    val docsSharedToLilJohn = docs.filter(_.sharedTo.contains(name))
    val sharedToLilJohnIds = fxfs(docsSharedToLilJohn).toSet
    val res = service.doSearch(params, AuthParams(cookie = Some(cookie)), Some(authenticatingDomain), None)

    // confirm that little john shares views on domains other than 0
    docsSharedToLilJohn.filter(_.socrataId.domainId != 0) should not be('empty)
    fxfs(res._2) should contain theSameElementsAs sharedToLilJohnIds
  }

  test("searching for assets owned by logged in user returns nothing if user owns nothing") {
    val authenticatingDomain = domains(0).domainCname
    val name = "No One"
    prepareAuthenticatedUser(cookie, authenticatingDomain, authedUserBodyWithoutRoleOrRights(name))

    val params = Map("for_user" -> name).mapValues(Seq(_))
    val res = service.doSearch(params, AuthParams(cookie = Some(cookie)), Some(authenticatingDomain), None)._2
    fxfs(res) should be('empty)
  }

  test("searching for assets shared to logged in user returns nothing if nothing is shared") {
    val authenticatingDomain = domains(0).domainCname
    val name = "No One"
    prepareAuthenticatedUser(cookie, authenticatingDomain, authedUserBodyWithoutRoleOrRights(name))

    val params = Map("shared_to" -> name).mapValues(Seq(_))
    val res = service.doSearch(params, AuthParams(cookie = Some(cookie)), Some(authenticatingDomain), None)._2
    fxfs(res) should be('empty)
  }

  test("searching for assets by sending for_user and shared_to params returns no results") {
    val authenticatingDomain = domains(0).domainCname
    prepareAuthenticatedUser(cookie, authenticatingDomain, authedUserBodyWithoutRoleOrRights("robin-hood"))

    val params = Map("shared_to" -> "robin-hood", "for_user" -> "robin-hood").mapValues(Seq(_))
    val res = service.doSearch(params, AuthParams(cookie = Some(cookie)), Some(authenticatingDomain), None)._2
    fxfs(res) should be('empty)
  }

  private def testRightlessUserApproval(domain: Domain, userId: String) = {
    val allPossibleResults = getAllPossibleResults()
    val approvedFxfs = allPossibleResults.filter(isApproved).map(fxfs)
    val visibleDomainsDocs = docs.filter(d =>
      (anonymouslyViewableDocIds.contains(d.socrataId.datasetId) || d.isSharedOrOwned(userId)) &&
      d.socrataId.domainId.toInt == domain.domainId)

    // approved views are views that pass all 3 types of approval
    val expectedApprovedFxfs = fxfs(visibleDomainsDocs.filter(d => approvedFxfs.contains(d.socrataId.datasetId)))
    // rejected views can't generally be seen by roleless users, unless they own/share the rejected view.
    val expectedRejectedFxfs = fxfs(visibleDomainsDocs.filter(d => d.isVmRejected || d.isRaRejected(domain.domainId)))
    // pending views can't generally be seen by roleless users, unless they own/share the pending view.
    val expectedPendingFxfs = fxfs(visibleDomainsDocs.filter(d => d.isVmPending || d.isRaPending(domain.domainId)))

    val host = domain.domainCname
    prepareAuthenticatedUser(cookie, host, authedUserBodyWithoutRoleOrRights(userId))

    val params = Map("search_context" -> host, "domains" -> host).mapValues(Seq(_))
    val actualApprovedFxfs = fxfs(service.doSearch(params ++ Map("approval_status" -> Seq("approved")),
      AuthParams(cookie=Some(cookie)), Some(host), None)._2)
    val actualRejectedFxfs = fxfs(service.doSearch(params ++ Map("approval_status" -> Seq("rejected")),
      AuthParams(cookie=Some(cookie)), Some(host), None)._2)
    val actualPendingFxfs = fxfs(service.doSearch(params ++ Map("approval_status" -> Seq("pending")),
      AuthParams(cookie=Some(cookie)), Some(host), None)._2)

    actualApprovedFxfs should contain theSameElementsAs expectedApprovedFxfs
    actualRejectedFxfs should contain theSameElementsAs expectedRejectedFxfs
    actualPendingFxfs should contain theSameElementsAs expectedPendingFxfs
  }

  test("searching on a basic domain (as both search_context & domain), should find the correct set of views for the given approval_status") {
    val basicDomain = domains(0)
    testRightlessUserApproval(basicDomain, "lil-john")
  }

  test("searching on a moderated domain (as both search_context & domain), should find the correct set of views for the given approval_status") {
    val moderatedDomain = domains(1)
    testRightlessUserApproval(moderatedDomain, "maid-marian" )
  }

  test("searching on an RA-enabled domain (as both search_context & domain), should find the correct set of views for the given approval_status") {
    val raDomain = domains(2)
    testRightlessUserApproval(raDomain, "robin-hood")
  }

  test("searching on a moderated & RA-enabled domain (as both search_context & domain), should find the correct set of views for the given approval_status") {
    val raVmDomain = domains(3)
    testRightlessUserApproval(raVmDomain, "prince-john")
  }

  test("searching on an unmoderated domain that federates in data from a moderated domain, should find the correct set of views for the given approval_status") {
    val unmoderatedDomain = domains(0).domainCname
    val moderatedDomain = domains(3).domainCname
    val userId = "lil-john"  // owns much on domain 0 and is shared a rejected and pending view on domain 3
    val domain0Docs = docs.filter(d => d.socrataId.domainId == 0)
    val domain3Docs = docs.filter(d => d.socrataId.domainId == 3)
    val ownedSharedOn0 = domain0Docs.filter(d => d.isSharedOrOwned(userId))
    val ownedSharedOn3 = domain3Docs.filter(d => d.isSharedOrOwned(userId))

    // on 0, views need not pass any approval. On 2, views must be view-moderated AND routing-approved
    val approvedOn0 = fxfs(domain0Docs.filter(d => anonymouslyViewableDocIds.contains(d.socrataId.datasetId)) ++ ownedSharedOn0)
    val approvedOn3 = fxfs(domain3Docs.filter(d => anonymouslyViewableDocIds.contains(d.socrataId.datasetId)) ++ ownedSharedOn3.filter(d => d.isVmApproved && d.isRaApproved(3)))
    val expectedApprovedFxfs = approvedOn0 ++ approvedOn3

    // on 0, rejected views can't be seen by roleless users unless they own/share the rejected view.
    val rejectedOn0 = fxfs(ownedSharedOn0.filter(d => d.isVmRejected))
    // on 3, rejected views can't be seen by roleless users unless they own/share the rejected view.
    val rejectedOn3 = fxfs(ownedSharedOn3.filter(d => d.isVmRejected || d.isRaRejected(3)))
    val expectedRejectedFxfs = rejectedOn0 ++ rejectedOn3

    // on 0, pending views can't be seen by roleless users unless they own/share the pending view.
    val pendingOn0 = fxfs(ownedSharedOn0.filter(d => d.isVmPending))
    // on 3, pending views can't be seen by roleless users unless they own/share the pending view.
    val pendingOn3 = fxfs(ownedSharedOn3.filter(d => d.isVmPending || d.isRaPending(3)))
    val expectedPendingFxfs = pendingOn0 ++ pendingOn3

    expectedApprovedFxfs shouldNot be('empty)
    expectedRejectedFxfs shouldNot be('empty)
    expectedPendingFxfs shouldNot be('empty)

    prepareAuthenticatedUser(cookie, unmoderatedDomain, authedUserBodyWithoutRoleOrRights(userId))

    val params = Map("search_context" -> Seq(unmoderatedDomain), "domains" -> Seq(s"$moderatedDomain,$unmoderatedDomain"))
    val actualApprovedFxfs = fxfs(service.doSearch(params ++ Map("approval_status" -> Seq("approved")),
      AuthParams(cookie=Some(cookie)), Some(unmoderatedDomain), None)._2)
    val actualRejectedFxfs = fxfs(service.doSearch(params ++ Map("approval_status" -> Seq("rejected")),
      AuthParams(cookie=Some(cookie)), Some(unmoderatedDomain), None)._2)
    val actualPendingFxfs = fxfs(service.doSearch(params ++ Map("approval_status" -> Seq("pending")),
      AuthParams(cookie=Some(cookie)), Some(unmoderatedDomain), None)._2)

    actualApprovedFxfs should contain theSameElementsAs expectedApprovedFxfs
    actualRejectedFxfs should contain theSameElementsAs expectedRejectedFxfs
    actualPendingFxfs should contain theSameElementsAs expectedPendingFxfs
  }

  test("searching on a moderated domain that federates in data from an unmoderated domain, should find the correct set of views for the given approval_status") {
    val moderatedDomain = domains(1).domainCname
    val unmoderatedDomain = domains(0).domainCname
    val userId = "friar-tuck"
    val domain1Docs = docs.filter(d => d.socrataId.domainId == 1)
    val domain0Docs = docs.filter(d => d.socrataId.domainId == 0)
    val ownedSharedOn1 = domain1Docs.filter(d => d.isSharedOrOwned(userId))

    // on 1, approved views are those that pass all 3 types of approval
    val approvedOn1 = fxfs(domain1Docs.filter(d => anonymouslyViewableDocIds.contains(d.socrataId.datasetId)))
    // on 0, b/c federating in unmoderated data to a moderated domain removes all derived views, only default views are approved
    val approvedOn0 = fxfs(domain0Docs.filter(d => anonymouslyViewableDocIds.contains(d.socrataId.datasetId) && d.isDefaultView))
    val expectedApprovedFxfs = approvedOn1 ++ approvedOn0

    // on 1, rejected views can't be seen by roleless users unless they own/share the rejected view
    val rejectedOn1 = fxfs(ownedSharedOn1.filter(d => d.isVmRejected))
    // on 0, nothing should come back rejected b/c federating in unmoderated data to a moderated domain removes all derived views
    val expectedRejectedFxfs = rejectedOn1

    // on 1, pending views can't be seen by roleless users unless they own/share the pending view
    val pendingOn1 = fxfs(ownedSharedOn1.filter(d => d.isVmPending))
    // on 0, nothing should come back pending b/c federating in unmoderated data to a moderated domain removes all derived views
    val expectedPendingFxfs = pendingOn1

    expectedApprovedFxfs shouldNot be('empty)
    expectedRejectedFxfs shouldNot be('empty)
    expectedPendingFxfs shouldNot be('empty)

    prepareAuthenticatedUser(cookie, moderatedDomain, authedUserBodyWithoutRoleOrRights(userId))

    val params = Map("search_context" -> Seq(moderatedDomain), "domains" -> Seq(s"$moderatedDomain,$unmoderatedDomain"))
    val actualApprovedFxfs = fxfs(service.doSearch(params ++ Map("approval_status" -> Seq("approved")),
      AuthParams(cookie=Some(cookie)), Some(moderatedDomain), None)._2)
    val actualRejectedFxfs = fxfs(service.doSearch(params ++ Map("approval_status" -> Seq("rejected")),
      AuthParams(cookie=Some(cookie)), Some(moderatedDomain), None)._2)
    val actualPendingFxfs = fxfs(service.doSearch(params ++ Map("approval_status" -> Seq("pending")),
      AuthParams(cookie=Some(cookie)), Some(moderatedDomain), None)._2)

    actualApprovedFxfs should contain theSameElementsAs expectedApprovedFxfs
    actualRejectedFxfs should contain theSameElementsAs expectedRejectedFxfs
    actualPendingFxfs should contain theSameElementsAs expectedPendingFxfs
  }

  test("searching on an RA-disabled domain that federates in data from an RA-enabled domain, should find the correct set of views for the given approval_status") {
    // domain 0 is an RA-disabled domain
    // domain 2 is an RA-enabled domain
    val raDisabledDomain = domains(0)
    val raEnabledDomain = domains(2)
    val raDisabledDomainCname = domains(0).domainCname
    val raEnabledDomainCname = domains(2).domainCname
    val userId = "maid-marian"
    val domain0Docs = docs.filter(d => d.socrataId.domainId == 0)
    val domain2Docs = docs.filter(d => d.socrataId.domainId == 2)
    val ownedSharedOn0 = domain0Docs.filter(d => d.isSharedOrOwned(userId))
    val ownedSharedOn2 = domain2Docs.filter(d => d.isSharedOrOwned(userId))

    val approvedOn0 = fxfs(domain0Docs.filter(d => anonymouslyViewableDocIds.contains(d.socrataId.datasetId)) ++ ownedSharedOn0)
    val approvedOn2 = fxfs(domain2Docs.filter(d => anonymouslyViewableDocIds.contains(d.socrataId.datasetId)) ++ ownedSharedOn2.filter(d => d.isRaApproved(2)))
    val expectedApprovedFxfs = approvedOn0 ++ approvedOn2

    // domain 0 has no rejected views because it has no form of moderation, so nothing is rejected
    // on 2, rejected views can't be seen by roleless users unless they own/share the rejected view
    val rejectedOn2 = fxfs(ownedSharedOn2.filter(d => d.isVmRejected || d.isRaRejected(2)))
    // confirm there are rejected views on domain 2 that could have come back
    domain2Docs.filter(d => d.isRejectedByParentDomain) shouldNot be('empty)
    val expectedRejectedFxfs = rejectedOn2

    // domain 0 has no pending views, as it has no form of moderation
    // on 1, pending views can't be seen by roleless users unless they own/share the pending view
    val pendingOn2 = fxfs(ownedSharedOn2.filter(d => d.isVmPending || d.isRaPending(2)))
    // confirm there are pending views on domain 1 that could have come back:
    domain2Docs.filter(d => d.isPendingOnParentDomain) shouldNot be('empty)
    val expectedPendingFxfs = pendingOn2

    expectedApprovedFxfs shouldNot be('empty)
    expectedRejectedFxfs shouldNot be('empty)
    expectedPendingFxfs shouldNot be('empty)

    prepareAuthenticatedUser(cookie, raDisabledDomainCname, authedUserBodyWithoutRoleOrRights(userId))

    val params = Map("search_context" -> Seq(raDisabledDomainCname), "domains" -> Seq(s"$raEnabledDomainCname,$raDisabledDomainCname"))
    val actualApprovedFxfs = fxfs(service.doSearch(params ++ Map("approval_status" -> Seq("approved")),
      AuthParams(cookie=Some(cookie)), Some(raDisabledDomainCname), None)._2)
    val actualRejectedFxfs = fxfs(service.doSearch(params ++ Map("approval_status" -> Seq("rejected")),
      AuthParams(cookie=Some(cookie)), Some(raDisabledDomainCname), None)._2)
    val actualPendingFxfs = fxfs(service.doSearch(params ++ Map("approval_status" -> Seq("pending")),
      AuthParams(cookie=Some(cookie)), Some(raDisabledDomainCname), None)._2)

    actualApprovedFxfs should contain theSameElementsAs expectedApprovedFxfs
    actualRejectedFxfs should contain theSameElementsAs expectedRejectedFxfs
    actualPendingFxfs should contain theSameElementsAs expectedPendingFxfs
  }

  test("searching on an RA-enabled domain that federates in data from an RA-disabled domain, should find the correct set of views for the given approval_status") {
    // domain 2 is an RA-enabled domain
    // domain 0 is an RA-disabled domain
    val raEnabledDomain = domains(2).domainCname
    val raDisabledDomain = domains(0).domainCname
    val userId = "maid-marian"
    val domain2Docs = docs.filter(d => d.socrataId.domainId == 2)
    val domain0Docs = docs.filter(d => d.socrataId.domainId == 0)
    val ownedSharedOn2 = domain2Docs.filter(d => d.isSharedOrOwned(userId))
    val ownedSharedOn0 = domain0Docs.filter(d => d.isSharedOrOwned(userId))

    // on 2, approved views are those that pass all 3 types of approval
    val approvedOn2 = fxfs(domain2Docs.filter(d => anonymouslyViewableDocIds.contains(d.socrataId.datasetId)))
    // on 0, approved views are those that pass all 3 types of approval and are approved by domain 2's RA Queue
    val approvedOn0 = fxfs(domain0Docs.filter(d => anonymouslyViewableDocIds.contains(d.socrataId.datasetId) && d.isRaApproved(2)))
    val expectedApprovedFxfs = approvedOn2 ++ approvedOn0

    // on 2, rejected views can't be seen by roleless users unless they own/share the rejected view
    val rejectedOn2 = fxfs(ownedSharedOn2.filter(d => d.isVmRejected || d.isRaRejected(2)))
    // on 0, rejected views can't be seen by roleless users unless they own/share the rejected view
    val rejectedOn0 = fxfs(ownedSharedOn0.filter(d => d.isVmRejected || d.isRaRejected(2)))
    val expectedRejectedFxfs = rejectedOn2 ++ rejectedOn0

    // on 2, pending views can't be seen by roleless users unless they own/share the pending view
    val pendingOn2 = fxfs(ownedSharedOn2.filter(d => d.isVmPending || d.isRaPending(2)))
    // on 0, pending views can't be seen by roleless users unless they own/share the pending view
    val pendingOn0 = fxfs(ownedSharedOn0.filter(d => d.isVmPending || d.isRaPending(2)))
    val expectedPendingFxfs = pendingOn2 ++ pendingOn0

    expectedApprovedFxfs shouldNot be('empty)
    expectedRejectedFxfs shouldNot be('empty)
    expectedPendingFxfs shouldNot be('empty)

    prepareAuthenticatedUser(cookie, raEnabledDomain, authedUserBodyWithoutRoleOrRights(userId))

    val params = Map("search_context" -> Seq(raEnabledDomain), "domains" -> Seq(s"$raEnabledDomain,$raDisabledDomain"))
    val actualApprovedFxfs = fxfs(service.doSearch(params ++ Map("approval_status" -> Seq("approved")),
      AuthParams(cookie=Some(cookie)), Some(raEnabledDomain), None)._2)
    val actualRejectedFxfs = fxfs(service.doSearch(params ++ Map("approval_status" -> Seq("rejected")),
      AuthParams(cookie=Some(cookie)), Some(raEnabledDomain), None)._2)
    val actualPendingFxfs = fxfs(service.doSearch(params ++ Map("approval_status" -> Seq("pending")),
      AuthParams(cookie=Some(cookie)), Some(raEnabledDomain), None)._2)

    actualApprovedFxfs should contain theSameElementsAs expectedApprovedFxfs
    actualRejectedFxfs should contain theSameElementsAs expectedRejectedFxfs
    actualPendingFxfs should contain theSameElementsAs expectedPendingFxfs
  }

  test("searching with the 'q' param finds no items where q matches the private metadata if the user doesn't own/share the doc") {
    val host = domains(0).domainCname
    val privateValue = "Cheetah Corp."
    prepareAuthenticatedUser(cookie, host, authedUserBodyWithoutRoleOrRights("no-one-we-know-about"))
    val params = allDomainsParams ++ Map("q" -> privateValue).mapValues(Seq(_))
    val res = service.doSearch(params, AuthParams(cookie=Some(cookie)), Some(host), None)
    val actualFxfs = fxfs(res._2)
    actualFxfs should be('empty)

    // confirm there were documents that were excluded.
    anonymouslyViewableDocs.find(_.socrataId.datasetId == "d0-v2").get.privateCustomerMetadataFlattened.exists(_.value == privateValue
    ) should be(true)
  }

  test("searching with the 'q' param finds items where q matches the private metadata if the user owns/shares the doc") {
    val host = domains(0).domainCname
    val privateValue = "Cheetah Corp."
    val expectedFxfs = fxfs(docs.filter(d =>
      d.privateCustomerMetadataFlattened.exists(m => m.value == privateValue &&
      d.owner.id == "robin-hood")))
    prepareAuthenticatedUser(cookie, host, robinHoodWithoutRole)
    val params = allDomainsParams ++ Map("q" -> privateValue).mapValues(Seq(_))
    val res = service.doSearch(params, AuthParams(cookie=Some(cookie)), Some(host), None)
    val actualFxfs = fxfs(res._2)
    actualFxfs should contain theSameElementsAs expectedFxfs
  }

  test("searching with a private metadata k/v pair param finds no items if the user doesn't own/share the doc") {
    val host = domains(0).domainCname
    val privateKey = "Secret domain 0 cat organization"
    val privateValue = "Cheetah Corp."
    prepareAuthenticatedUser(cookie, host, authedUserBodyWithoutRoleOrRights("no-one-we-know-about"))
    val params = allDomainsParams ++ Map(privateKey -> Seq(privateValue))
    val res = service.doSearch(params, AuthParams(cookie=Some(cookie)), Some(host), None)
    val actualFxfs = fxfs(res._2)
    actualFxfs should be('empty)

    // confirm there were documents that were excluded.
    anonymouslyViewableDocs.find(_.socrataId.datasetId == "d0-v2").get.privateCustomerMetadataFlattened.exists(m =>
      m.value == privateValue && m.key == privateKey) should be(true)
  }

  test("searching with a private metadata k/v pair param finds items if the user owns/shares the doc") {
    val host = domains(0).domainCname
    val privateKey = "Secret domain 0 cat organization"
    val privateValue = "Cheetah Corp."
    val expectedFxfs = fxfs(docs.filter(d =>
      d.privateCustomerMetadataFlattened.exists(m => m.value == privateValue &&
      d.owner.id == "robin-hood")))
    prepareAuthenticatedUser(cookie, host, robinHoodWithoutRole)
    val params = allDomainsParams ++ Map(privateKey -> Seq(privateValue))
    val res = service.doSearch(params, AuthParams(cookie=Some(cookie)), Some(host), None)
    val actualFxfs = fxfs(res._2)
    actualFxfs should contain theSameElementsAs expectedFxfs
  }

  test("searching with 'visibility=open' filters away the internal items the user can view") {
    val host = domains(0).domainCname
    prepareAuthenticatedUser(cookie, host, robinHoodWithoutRole)
    val params = Map("domains" -> host).mapValues(Seq(_))
    val allRes = service.doSearch(params, AuthParams(cookie=Some(cookie)), Some(host), None)
    val allFxfs = fxfs(allRes._2)
    val (openFxfs, internalFxfs) = allFxfs.partition(anonymouslyViewableDocIds.contains(_))

    val openParams = Map("domains" -> host, "visibility" -> "open").mapValues(Seq(_))
    val openRes = service.doSearch(openParams, AuthParams(cookie=Some(cookie)), Some(host), None)
    val actualFxfs = fxfs(openRes._2)

    // confirm that only open docs are returned and that there were internal docs removed.
    actualFxfs should contain theSameElementsAs openFxfs
    internalFxfs.nonEmpty should be(true)
  }

  test("searching with 'visibility=internal' filters away the open items the user can view") {
    val host = domains(0).domainCname
    prepareAuthenticatedUser(cookie, host, robinHoodWithoutRole)
    val params = Map("domains" -> host).mapValues(Seq(_))
    val allRes = service.doSearch(params, AuthParams(cookie=Some(cookie)), Some(host), None)
    val allFxfs = fxfs(allRes._2)
    val (openFxfs, internalFxfs) = allFxfs.partition(anonymouslyViewableDocIds.contains(_))

    val internalParams = Map("domains" -> host, "visibility" -> "internal").mapValues(Seq(_))
    val internalRes = service.doSearch(internalParams, AuthParams(cookie=Some(cookie)), Some(host), None)
    val actualFxfs = fxfs(internalRes._2)

    // confirm that only internal docs are returned and that there were open docs removed.
    actualFxfs should contain theSameElementsAs internalFxfs
    openFxfs.nonEmpty should be(true)
  }
}
