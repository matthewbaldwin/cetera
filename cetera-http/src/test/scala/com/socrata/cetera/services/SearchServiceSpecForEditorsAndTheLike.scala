package com.socrata.cetera.services

import com.rojoma.json.v3.interpolation._
import org.scalatest.{BeforeAndAfterAll, BeforeAndAfterEach, FunSuiteLike, Matchers}

import com.socrata.cetera.auth.AuthParams
import com.socrata.cetera.types.{Document, Domain}
import com.socrata.cetera.{response => _, _}

class SearchServiceSpecForEditorsAndTheLike
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

  test("searching across all domains with an editor shows " +
    "a) anonymously visible views from their domain " +
    "b) anonymously visible views from unlocked domains " +
    "c) views they own/share") {
    val authenticatingDomain = domains(0)
    val ownedByCookieMonster = docs.collect{ case d: Document if d.ownerId == "cook-mons" => d.socrataId.datasetId }
    val sharedToCookieMonster = docs.collect{ case d: Document if d.sharedTo.contains("cook-mons") => d.socrataId.datasetId }
    val expectedFxfs = (ownedByCookieMonster ++ sharedToCookieMonster ++ anonymouslyViewableDocIds).distinct

    val host = domains(0).domainCname
    val editorBody = authedUserBodyFromRole("editor")
    prepareAuthenticatedUser(cookie, host, editorBody)
    val editorRes = service.doSearch(allDomainsParams, AuthParams(cookie = Some(cookie)), Some(host), None)
    fxfs(editorRes._2) should contain theSameElementsAs expectedFxfs
  }

  test("searching across all domains when auth is required with a roleless, but logged-in users shows " +
    "a) anonymously visible views " +
    "b) views they own/share") {
    val authenticatingDomain = domains(0)
    val ownedByCookieMonster = docs.collect{ case d: Document if d.ownerId == "cook-mons" => d.socrataId.datasetId }
    val sharedToCookieMonster = docs.collect{ case d: Document if d.sharedTo.contains("cook-mons") => d.socrataId.datasetId }
    val expectedFxfs = (ownedByCookieMonster ++ sharedToCookieMonster ++ anonymouslyViewableDocIds).distinct

    val host = domains(0).domainCname
    val editorBody = authedUserBodyFromRole("")
    prepareAuthenticatedUser(cookie, host, editorBody)
    val editorRes = service.doSearch(allDomainsParams, AuthParams(cookie = Some(cookie)), Some(host), None)
    fxfs(editorRes._2) should contain theSameElementsAs expectedFxfs
  }

  test("searching on a locked domains with an editor shows data") {
    val lockedDomain = domains(8).domainCname
    val params = Map(
      "domains" -> lockedDomain,
      "search_context" -> lockedDomain
    ).mapValues(Seq(_))
    val expectedFxfs = fxfs(docs.filter(d => d.socrataId.domainId == 8))
    val editorBody = authedUserBodyFromRole("editor")
    prepareAuthenticatedUser(cookie, lockedDomain, editorBody)
    val res = service.doSearch(params, AuthParams(cookie = Some(cookie)), Some(lockedDomain), None)._2
    fxfs(res) should contain theSameElementsAs expectedFxfs
  }

  test("searching on a locked domains with a roleless user though, should show nothing") {
    val lockedDomain = domains(8).domainCname
    val params = Map(
      "domains" -> lockedDomain,
      "search_context" -> lockedDomain
    ).mapValues(Seq(_))

    val userBody = authedUserBodyFromRole("")
    prepareAuthenticatedUser(cookie, lockedDomain, userBody)
    val res = service.doSearch(params, AuthParams(cookie = Some(cookie)), Some(lockedDomain), None)._2
    fxfs(res) should be('empty)
  }

  test("searching for assets owned by logged in user returns all assets the user owns, " +
    "regardless of private/public/approval status and regardless of what domain they come from") {
    // this has robin-hood looking up his own data.
    val authenticatingDomain = domains(0).domainCname
    val params = Map("for_user" -> Seq("robin-hood"))
    val userBody = j"""{"id" : "robin-hood"}"""
    prepareAuthenticatedUser(cookie, authenticatingDomain, userBody)

    val docsOwnedByRobin = docs.filter(_.ownerId == "robin-hood")
    val ownedByRobinIds = fxfs(docsOwnedByRobin).toSet
    val res = service.doSearch(params, AuthParams(cookie = Some(cookie)), Some(authenticatingDomain), None)

    // confirm that robin owns view on domains other than 0
    docsOwnedByRobin.filter(_.socrataId.domainId !=0) should not be('empty)
    fxfs(res._2) should contain theSameElementsAs ownedByRobinIds
  }

  test("searching for assets shared to logged in user works if a public/published asset is shared") {
    val host = domains(0).domainCname
    val authedUserBody =
      j"""{
        "id" : "King Richard",
        "roleName" : "King",
        "rights" : [ "collect_taxes", "wear_crown" ]
        }"""
    val expectedFxfs = Seq("zeta-0007")

    prepareAuthenticatedUser(cookie, host, authedUserBody)
    val params = Map("domains" -> host, "search_context" -> host, "shared_to" -> "King Richard").mapValues(Seq(_))
    val res = service.doSearch(params, AuthParams(cookie = Some(cookie)), Some(host), None)._2
    fxfs(res) should contain theSameElementsAs expectedFxfs
  }

  test("searching for assets shared to logged in user works if a private/unpublished asset is shared") {
    val host = domains(0).domainCname
    val authedUserBody =
      j"""{
        "id" : "Little John",
        "roleName" : "secondInCommand",
        "rights" : [ "rob_from_the_rich", "give_to_the_poor" ]
        }"""
    val expectedFxfs = Seq("zeta-0006")

    prepareAuthenticatedUser(cookie, host, authedUserBody)
    val params = Map("domains" -> host, "search_context" -> host, "shared_to" -> "Little John").mapValues(Seq(_))
    val res = service.doSearch(params, AuthParams(cookie = Some(cookie)), Some(host), None)._2
    fxfs(res) should contain theSameElementsAs expectedFxfs
  }

  test("searching for assets owned by logged in user returns nothing if user owns nothing") {
    val host = domains(0).domainCname
    val authedUserBody =
      j"""{
        "id" : "No One",
        "roleName" : "nothing",
        "rights" : [ "nothing" ]
        }"""
    val expectedFxfs = None

    prepareAuthenticatedUser(cookie, host, authedUserBody)
    val params = Map("domains" -> host, "search_context" -> host, "shared_to" -> "No One").mapValues(Seq(_))
    val res = service.doSearch(params, AuthParams(cookie = Some(cookie)), Some(host), None)._2
    fxfs(res) should contain theSameElementsAs expectedFxfs
  }

  test("searching for assets shared to logged in user returns nothing if nothing is shared") {
    val host = domains(0).domainCname
    val authedUserBody =
      j"""{
        "id" : "No One",
        "roleName" : "nothing",
        "rights" : [ "nothing" ]
        }"""
    val expectedFxfs = None

    prepareAuthenticatedUser(cookie, host, authedUserBody)
    val params = Map("domains" -> host, "search_context" -> host, "shared_to" -> "No One").mapValues(Seq(_))
    val res = service.doSearch(params, AuthParams(cookie = Some(cookie)), Some(host), None)._2
    fxfs(res) should contain theSameElementsAs expectedFxfs
  }

  test("searching for assets by sending for_user and shared_to params returns no results") {
    val host = "petercetera.net"
    val authedUserBody =
      j"""{
        "id" : "robin-hood",
        "roleName" : "leader",
        "rights" : [ "rob_from_the_rich", "give_to_the_poor", "get_all_the_glory" ]
        }"""

    prepareAuthenticatedUser(cookie, host, authedUserBody)
    val params = Map("domains" -> host, "search_context" -> host, "shared_to" -> "robin-hood", "for_user" -> "robin-hood").mapValues(Seq(_))
    val res = service.doSearch(params, AuthParams(cookie = Some(cookie)), Some(host), None)._2
    fxfs(res) should be('empty)
  }

  private def testRolelessUserApproval(domain: Domain, userId: String) = {
    val allPossibleResults = getAllPossibleResults()
    val approvedFxfs = allPossibleResults.filter(isApproved).map(fxfs)
    val visibleDomainsDocs = docs.filter(d =>
      (anonymouslyViewableDocIds.contains(d.socrataId.datasetId) || d.isSharedOrOwned(userId)) &&
      d.socrataId.domainId == domain.domainId)

    // approved views are views that pass all 3 types of approval
    val expectedApprovedFxfs = fxfs(visibleDomainsDocs.filter(d => approvedFxfs.contains(d.socrataId.datasetId)))
    // rejected views can't generally be seen by roleless users, unless they own/share the rejected view.
    val expectedRejectedFxfs = fxfs(visibleDomainsDocs.filter(d => d.isVmRejected || d.isRaRejected(domain.domainId)))
    // pending views can't generally be seen by roleless users, unless they own/share the pending view.
    val expectedPendingFxfs = fxfs(visibleDomainsDocs.filter(d => d.isVmPending || d.isRaPending(domain.domainId)))

    expectedApprovedFxfs shouldNot be('empty)
    expectedRejectedFxfs shouldNot be('empty)
    expectedPendingFxfs shouldNot be('empty)

    val host = domain.domainCname
    val userBody = authedUserBodyFromRole("", userId)
    prepareAuthenticatedUser(cookie, host, userBody)

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

  test("searching as a roleless user on a basic domain (as both search_context & domain), should find the correct set of views for the given approval_status") {
    val basicDomain = domains(0)
    testRolelessUserApproval(basicDomain, "lil-john")
  }

  test("searching as a roleless user on a moderated domain (as both search_context & domain), should find the correct set of views for the given approval_status") {
    val moderatedDomain = domains(1)
    testRolelessUserApproval(moderatedDomain, "maid-marian" )
  }

  test("searching as a roleless user on an RA-enabled domain (as both search_context & domain), should find the correct set of views for the given approval_status") {
    val raDomain = domains(2)
    testRolelessUserApproval(raDomain, "robin-hood")
  }

  test("searching as a roleless user on a moderated & RA-enabled domain (as both search_context & domain), should find the correct set of views for the given approval_status") {
    val raVmDomain = domains(3)
    testRolelessUserApproval(raVmDomain, "prince-john")
  }

  test("searching as a roleless user on an unmoderated domain that federates in data from a moderated domain, should find the correct set of views for the given approval_status") {
    val unmoderatedDomain = domains(0).domainCname
    val moderatedDomain = domains(3).domainCname
    val userId = "lil-john"  // owns much on domain 0 and is shared a rejected and pending view on domain 3
    val domain0Docs = docs.filter(d => d.socrataId.domainId == 0)
    val domain3Docs = docs.filter(d => d.socrataId.domainId == 3)
    val ownedSharedOn0 = domain0Docs.filter(d => d.isSharedOrOwned(userId))
    val ownedSharedOn3 = domain3Docs.filter(d => d.isSharedOrOwned(userId))

    // on both 0 and 1, approved views are those that pass all 3 types of approval
    val approvedOn0 = fxfs(domain0Docs.filter(d => anonymouslyViewableDocIds.contains(d.socrataId.datasetId)))
    val approvedOn3 = fxfs(domain3Docs.filter(d => anonymouslyViewableDocIds.contains(d.socrataId.datasetId)))
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

    val userBody = authedUserBodyFromRole("", userId)
    prepareAuthenticatedUser(cookie, unmoderatedDomain, userBody)

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

  test("searching as a roleless user on a moderated domain that federates in data from an unmoderated domain, should find the correct set of views for the given approval_status") {
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
    val rejectedOn0 = List.empty
    // confirm there are rejected views on domain 0 that could have come back:
    domain0Docs.filter(d => d.isVmRejected) shouldNot be('empty)
    val expectedRejectedFxfs = rejectedOn1 ++ rejectedOn0

    // on 1, pending views can't be seen by roleless users unless they own/share the pending view
    val pendingOn1 = fxfs(ownedSharedOn1.filter(d => d.isVmPending))
    // on 0, nothing should come back pending b/c federating in unmoderated data to a moderated domain removes all derived views,
    val pendingOn0 = List.empty
    // confirm there are pending views on domain 0 that could have come back:
    domain0Docs.filter(d => d.isVmPending) shouldNot be('empty)
    val expectedPendingFxfs = pendingOn1 ++ pendingOn0

    expectedApprovedFxfs shouldNot be('empty)
    expectedRejectedFxfs shouldNot be('empty)
    expectedPendingFxfs shouldNot be('empty)

    val userBody = authedUserBodyFromRole("", userId)
    prepareAuthenticatedUser(cookie, moderatedDomain, userBody)

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

  test("searching as a roleless user on an RA-disabled domain that federates in data from an RA-enabled domain, should find the correct set of views for the given approval_status") {
    // domain 0 is an RA-disabled domain
    // domain 2 is an RA-enabled domain
    val raDisabledDomain = domains(0).domainCname
    val raEnabledDomain = domains(2).domainCname
    val userId = "maid-marian"
    val domain0Docs = docs.filter(d => d.socrataId.domainId == 0)
    val domain2Docs = docs.filter(d => d.socrataId.domainId == 2)
    val ownedSharedOn0 = domain0Docs.filter(d => d.isSharedOrOwned(userId))
    val ownedSharedOn2 = domain2Docs.filter(d => d.isSharedOrOwned(userId))

    // on both 0 and 2, approved views are those that pass all 3 types of approval
    val approvedOn0 = fxfs(domain0Docs.filter(d => anonymouslyViewableDocIds.contains(d.socrataId.datasetId)))
    val approvedOn2 = fxfs(domain2Docs.filter(d => anonymouslyViewableDocIds.contains(d.socrataId.datasetId)))
    val expectedApprovedFxfs = approvedOn0 ++ approvedOn2

    // on 0, rejected views can't be seen by roleless users unless they own/share the rejected view
    val rejectedOn0 = fxfs(ownedSharedOn0.filter(d => d.datatype.startsWith("datalens") && d.isVmRejected))
    // on 2, rejected views can't be seen by roleless users unless they own/share the rejected view
    val rejectedOn2 = fxfs(ownedSharedOn2.filter(d => d.isVmRejected || d.isRaRejected(2)))
    // confirm there are rejected views on domain 2 that could have come back
    domain2Docs.filter(d => d.isRejectedByParentDomain) shouldNot be('empty)
    val expectedRejectedFxfs = rejectedOn0 ++ rejectedOn2

    // on 0, pending views can't be seen by roleless users unless they own/share the pending view
    val pendingOn0 = fxfs(ownedSharedOn0.filter(d => d.datatype.startsWith("datalens") && d.isVmPending))
    // on 1, pending views can't be seen by roleless users unless they own/share the pending view
    val pendingOn2 = fxfs(ownedSharedOn2.filter(d => d.isVmPending || d.isRaPending(2)))
    // confirm there are pending views on domain 1 that could have come back:
    domain2Docs.filter(d => d.isPendingOnParentDomain) shouldNot be('empty)
    val expectedPendingFxfs = pendingOn0 ++ pendingOn2

    expectedApprovedFxfs shouldNot be('empty)
    expectedRejectedFxfs shouldNot be('empty)
    expectedPendingFxfs shouldNot be('empty)

    val userBody = authedUserBodyFromRole("", userId)
    prepareAuthenticatedUser(cookie, raDisabledDomain, userBody)

    val params = Map("search_context" -> Seq(raDisabledDomain), "domains" -> Seq(s"$raEnabledDomain,$raDisabledDomain"))
    val actualApprovedFxfs = fxfs(service.doSearch(params ++ Map("approval_status" -> Seq("approved")),
      AuthParams(cookie=Some(cookie)), Some(raDisabledDomain), None)._2)
    val actualRejectedFxfs = fxfs(service.doSearch(params ++ Map("approval_status" -> Seq("rejected")),
      AuthParams(cookie=Some(cookie)), Some(raDisabledDomain), None)._2)
    val actualPendingFxfs = fxfs(service.doSearch(params ++ Map("approval_status" -> Seq("pending")),
      AuthParams(cookie=Some(cookie)), Some(raDisabledDomain), None)._2)

    actualApprovedFxfs should contain theSameElementsAs expectedApprovedFxfs
    actualRejectedFxfs should contain theSameElementsAs expectedRejectedFxfs
    actualPendingFxfs should contain theSameElementsAs expectedPendingFxfs
  }

  test("searching as a roleless user on an RA-enabled domain that federates in data from an RA-disabled domain, should find the correct set of views for the given approval_status") {
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

    val userBody = authedUserBodyFromRole("", userId)
    prepareAuthenticatedUser(cookie, raEnabledDomain, userBody)

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
    val userBody = authedUserBodyFromRole("editor")
    prepareAuthenticatedUser(cookie, host, userBody)
    val params = allDomainsParams ++ Map("q" -> privateValue).mapValues(Seq(_))
    val res = service.doSearch(params, AuthParams(cookie=Some(cookie)), Some(host), None)
    val actualFxfs = fxfs(res._2)
    actualFxfs should be('empty)

    // confirm there were documents that were excluded.
    anonymouslyViewableDocs.find(_.socrataId.datasetId == "fxf-8").get.privateCustomerMetadataFlattened.exists(_.value == privateValue
    ) should be(true)
  }

  test("searching with the 'q' param finds items where q matches the private metadata if the user owns/shares the doc") {
    val host = domains(0).domainCname
    val privateValue = "Cheetah Corp."
    val expectedFxfs = fxfs(docs.filter(d =>
      d.privateCustomerMetadataFlattened.exists(m => m.value == privateValue &&
      d.ownerId == "robin-hood")))
    val userBody = authedUserBodyFromRole("editor", "robin-hood")
    prepareAuthenticatedUser(cookie, host, userBody)
    val params = allDomainsParams ++ Map("q" -> privateValue).mapValues(Seq(_))
    val res = service.doSearch(params, AuthParams(cookie=Some(cookie)), Some(host), None)
    val actualFxfs = fxfs(res._2)
    actualFxfs should contain theSameElementsAs expectedFxfs
  }

  test("searching with a private metadata k/v pair param finds no items if the user doesn't own/share the doc") {
    val host = domains(0).domainCname
    val privateKey = "Secret domain 0 cat organization"
    val privateValue = "Cheetah Corp."
    val userBody = authedUserBodyFromRole("editor")
    prepareAuthenticatedUser(cookie, host, userBody)
    val params = allDomainsParams ++ Map(privateKey -> Seq(privateValue))
    val res = service.doSearch(params, AuthParams(cookie=Some(cookie)), Some(host), None)
    val actualFxfs = fxfs(res._2)
    actualFxfs should be('empty)

    // confirm there were documents that were excluded.
    anonymouslyViewableDocs.find(_.socrataId.datasetId == "fxf-8").get.privateCustomerMetadataFlattened.exists(m =>
      m.value == privateValue && m.key == privateKey) should be(true)
  }

  test("searching with a private metadata k/v pair param finds items if the user owns/shares the doc") {
    val host = domains(0).domainCname
    val privateKey = "Secret domain 0 cat organization"
    val privateValue = "Cheetah Corp."
    val expectedFxfs = fxfs(docs.filter(d =>
      d.privateCustomerMetadataFlattened.exists(m => m.value == privateValue &&
      d.ownerId == "robin-hood")))
    val userBody = authedUserBodyFromRole("editor", "robin-hood")
    prepareAuthenticatedUser(cookie, host, userBody)
    val params = allDomainsParams ++ Map(privateKey -> Seq(privateValue))
    val res = service.doSearch(params, AuthParams(cookie=Some(cookie)), Some(host), None)
    val actualFxfs = fxfs(res._2)
    actualFxfs should contain theSameElementsAs expectedFxfs
  }

  test("searching with 'visibility=open' filters away the internal items the user can view") {
    val host = domains(0).domainCname
    val userBody = authedUserBodyFromRole("editor", "robin-hood")
    prepareAuthenticatedUser(cookie, host, userBody)
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
    val userBody = authedUserBodyFromRole("editor", "robin-hood")
    prepareAuthenticatedUser(cookie, host, userBody)
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
