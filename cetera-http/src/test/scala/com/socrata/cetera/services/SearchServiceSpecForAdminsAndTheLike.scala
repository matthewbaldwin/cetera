package com.socrata.cetera.services

import com.rojoma.json.v3.ast.JValue
import org.scalatest.{BeforeAndAfterAll, BeforeAndAfterEach, FunSuiteLike, Matchers}

import com.socrata.cetera.auth.AuthParams
import com.socrata.cetera.handlers.Params
import com.socrata.cetera.types.{Document, DomainSet}
import com.socrata.cetera.{response => _, _}

class SearchServiceSpecForAdminsAndTheLike
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

  test("searching across all domains with an administrator/publisher/designer/viewer" +
    "a) everything from their domain" +
    "b) anonymously visible views from unlocked domains" +
    "c) views they own/share") {
    val raDisabledDomain = domains(0)
    val withinDomain = docs.filter(d => d.socrataId.domainId == raDisabledDomain.domainId).map(d => d.socrataId.datasetId)
    val ownedByCookieMonster = docs.collect{ case d: Document if d.owner.id == "cook-mons" => d.socrataId.datasetId }
    ownedByCookieMonster should be(List("zeta-0006"))
    val sharedToCookieMonster = docs.collect{ case d: Document if d.sharedTo.contains("cook-mons") => d.socrataId.datasetId }
    sharedToCookieMonster should be(List("zeta-0004"))
    val expectedFxfs = (withinDomain ++ ownedByCookieMonster ++ sharedToCookieMonster ++ anonymouslyViewableDocIds).distinct

    val host = raDisabledDomain.domainCname
    val adminBody = authedUserBodyFromRole("administrator")
    prepareAuthenticatedUser(cookie, host, adminBody)
    val adminRes = service.doSearch(allDomainsParams, AuthParams(cookie = Some(cookie)), Some(host), None)
    fxfs(adminRes._2) should contain theSameElementsAs expectedFxfs

    val publisherBody = authedUserBodyFromRole("publisher")
    prepareAuthenticatedUser(cookie, host, publisherBody)
    val publisherRes = service.doSearch(allDomainsParams, AuthParams(cookie = Some(cookie)), Some(host), None)
    fxfs(publisherRes._2) should contain theSameElementsAs expectedFxfs

    val designerBody = authedUserBodyFromRole("designer")
    prepareAuthenticatedUser(cookie, host, designerBody)
    val designerRes = service.doSearch(allDomainsParams, AuthParams(cookie = Some(cookie)), Some(host), None)
    fxfs(designerRes._2) should contain theSameElementsAs expectedFxfs

    val viewerBody = authedUserBodyFromRole("viewer")
    prepareAuthenticatedUser(cookie, host, viewerBody)
    val viewerRes = service.doSearch(allDomainsParams, AuthParams(cookie = Some(cookie)), Some(host), None)
    fxfs(viewerRes._2) should contain theSameElementsAs expectedFxfs
  }

  test("searching on a locked domains with an administrator/publisher/viewer shows data") {
    val lockedDomain = domains(8).domainCname
      val params = Map(
        "domains" -> lockedDomain,
        "search_context" -> lockedDomain
      ).mapValues(Seq(_))
    val expectedFxfs = fxfs(docs.filter(d => d.socrataId.domainId == 8))

    val adminBody = authedUserBodyFromRole("administrator")
    prepareAuthenticatedUser(cookie, lockedDomain, adminBody)
    val adminRes = service.doSearch(params, AuthParams(cookie = Some(cookie)), Some(lockedDomain), None)._2
    fxfs(adminRes) should contain theSameElementsAs expectedFxfs

    val publisherBody = authedUserBodyFromRole("publisher")
    prepareAuthenticatedUser(cookie, lockedDomain, publisherBody)
    val publisherRes = service.doSearch(params, AuthParams(cookie = Some(cookie)), Some(lockedDomain), None)._2
    fxfs(publisherRes) should contain theSameElementsAs expectedFxfs

    val viewerBody = authedUserBodyFromRole("viewer")
    prepareAuthenticatedUser(cookie, lockedDomain, viewerBody)
    val viewerRes = service.doSearch(params, AuthParams(cookie = Some(cookie)), Some(lockedDomain), None)._2
    fxfs(viewerRes) should contain theSameElementsAs expectedFxfs
  }

  test("searching on a locked domains with a designer though, should show nothing") {
    val lockedDomain = domains(8).domainCname
    val params = Map(
      "domains" -> lockedDomain,
      "search_context" -> lockedDomain
    ).mapValues(Seq(_))

    val designerBody = authedUserBodyFromRole("designer")
    prepareAuthenticatedUser(cookie, lockedDomain, designerBody)
    val res = service.doSearch(params, AuthParams(cookie = Some(cookie)), Some(lockedDomain), None)._2
    fxfs(res) should be('empty)
  }

  test("searching for assets owned by another user (robin-hood) when the logged-in user (cook-mons) is an admin returns " +
    " a) any views that robin-hood owns on cook-mons' domain" +
    " b) anonymously viewable views on any unlocked domain") {
    // this has cook-mons (admin on domain 0) checking up on robin-hood
    val authenticatingDomain = domains(0).domainCname
    val params = Map("for_user" -> Seq("robin-hood"))
    prepareAuthenticatedUser(cookie, authenticatingDomain, authedUserBodyFromRole("administrator"))

    val ownedByRobin = docs.collect{ case d: Document if d.owner.id == "robin-hood" => d.socrataId.datasetId }.toSet
    val onDomain0 = docs.collect{ case d: Document if d.socrataId.domainId == 0 => d.socrataId.datasetId}.toSet
    val anonymouslyViewable = anonymouslyViewableDocIds.toSet
    val expectedFxfs = ownedByRobin & (onDomain0 ++ anonymouslyViewable)

    val res = service.doSearch(params, AuthParams(cookie = Some(cookie)), Some(authenticatingDomain), None)

    // confirm that robin has documents on other domains that are being excluded
    expectedFxfs.size < ownedByRobin.size should be(true)
    fxfs(res._2) should contain theSameElementsAs expectedFxfs
  }

  test("hidden documents should not be hidden to users who can see everything on their domain") {
    val host = domains(0).domainCname
    val hiddenDoc = docs(4)
    val userBody = authedUserBodyFromRole("publisher")
    prepareAuthenticatedUser(cookie, host, userBody)

    val (_, res, _, _) = service.doSearch(Map(
      Params.ids -> hiddenDoc.socrataId.datasetId
    ).mapValues(Seq(_)), AuthParams(cookie = Some(cookie)), Some(host), None)
    val actualFxfs = fxfs(res)
    actualFxfs(0) should be(hiddenDoc.socrataId.datasetId)
  }

  def testApprovalStatusFxfs(domainSet: DomainSet, userBody: JValue, expectedApprovedFxfs: Seq[String],
      expectedRejectedFxfs: Seq[String], expectedPendingFxfs: Seq[String]): Unit = {
    val context = domainSet.searchContext.get.domainCname
    val doms = domainSet.domains.map(_.domainCname).mkString(",")

    expectedApprovedFxfs shouldNot be('empty)
    expectedRejectedFxfs shouldNot be('empty)
    expectedPendingFxfs shouldNot be('empty)

    prepareAuthenticatedUser(cookie, context, userBody)

    val params = Map("search_context" -> Seq(context), "domains" -> Seq(doms))
    val actualApprovedFxfs = fxfs(service.doSearch(params ++ Map("approval_status" -> Seq("approved")),
      AuthParams(cookie=Some(cookie)), Some(context), None)._2)
    val actualRejectedFxfs = fxfs(service.doSearch(params ++ Map("approval_status" -> Seq("rejected")),
      AuthParams(cookie=Some(cookie)), Some(context), None)._2)
    val actualPendingFxfs = fxfs(service.doSearch(params ++ Map("approval_status" -> Seq("pending")),
      AuthParams(cookie=Some(cookie)), Some(context), None)._2)

    actualApprovedFxfs should contain theSameElementsAs expectedApprovedFxfs
    actualRejectedFxfs should contain theSameElementsAs expectedRejectedFxfs
    actualPendingFxfs should contain theSameElementsAs expectedPendingFxfs
  }

  test("searching as an admin on a basic domain (as both search_context & domain), should find the correct set of views for the given approval_status") {
    // domain 0 has approved/pending/rejected datalenses
    val basicDomain = domains(0)
    val allPossibleResults = getAllPossibleResults()
    val domain0Docs = docs.filter(d => d.socrataId.domainId == 0)
    val approvedFxfs = allPossibleResults.filter(isApproved).map(fxfs)
    val datalenses = domain0Docs.filter(d => d.datatype.startsWith("datalens"))

    // approved views are views that pass all 3 types of approval (though in this case, only two are relevant)
    val expectedApprovedFxfs = fxfs(domain0Docs.filter(d => approvedFxfs.contains(d.socrataId.datasetId)))
    // rejected views can only be rejected datalens, since there is no mechanism to reject anything else (TODO: change when introduce RA)
    val expectedRejectedFxfs = fxfs(datalenses.filter(d => d.isVmRejected))
    // pending views can only be pending datalens, since there is no means for anything else to be pending (TODO: change when introduce RA)
    val expectedPendingFxfs = fxfs(datalenses.filter(d => d.isVmPending))

    val domainSet = DomainSet(Set(basicDomain), Some(basicDomain))
    val userBody = authedUserBodyFromRole("administrator")
    testApprovalStatusFxfs(domainSet, userBody, expectedApprovedFxfs, expectedRejectedFxfs, expectedPendingFxfs)
  }

  test("searching as an admin on a moderated domain (as both search_context & domain), should find the correct set of views for the given approval_status") {
    val moderatedDomain = domains(1)
    val allPossibleResults = getAllPossibleResults()
    val approvedFxfs = allPossibleResults.filter(isApproved).map(fxfs)
    val domain1Docs = docs.filter(d => d.socrataId.domainId == 1)
    // approved views are views that pass all 3 types of approval
    val expectedApprovedFxfs = fxfs(domain1Docs.filter(d => approvedFxfs.contains(d.socrataId.datasetId)))
    // rejected views are simply any views that are rejected
    val expectedRejectedFxfs = fxfs(domain1Docs.filter(d => d.isVmRejected))
    // pending views are simply any views that are pending
    val expectedPendingFxfs = fxfs(domain1Docs.filter(d => d.isVmPending))

    val domainSet = DomainSet(Set(moderatedDomain), Some(moderatedDomain))
    val userBody = authedUserBodyFromRole("administrator")
    testApprovalStatusFxfs(domainSet, userBody, expectedApprovedFxfs, expectedRejectedFxfs, expectedPendingFxfs)
  }

  test("searching as an admin on an RA-enabled domain (as both search_context & domain), should find the correct set of views for the given approval_status") {
    val raDomain = domains(2)
    val allPossibleResults = getAllPossibleResults()
    val approvedFxfs = allPossibleResults.filter(isApproved).map(fxfs)
    val domain2Docs = docs.filter(d => d.socrataId.domainId == 2)
    // approved views are views that pass all 3 types of approval
    val expectedApprovedFxfs = fxfs(domain2Docs.filter(d => approvedFxfs.contains(d.socrataId.datasetId)))
    // rejected views are those rejected as a DL or rejected by the context's RA process
    val expectedRejectedFxfs = fxfs(domain2Docs.filter(d => d.isRejectedByParentDomain || d.isVmRejected))
    // pending views are those pending as a DL or pending by the context's RA process
    val expectedPendingFxfs = fxfs(domain2Docs.filter(d => d.isPendingOnParentDomain || d.isVmPending))

    val domainSet = DomainSet(Set(raDomain), Some(raDomain))
    val userBody = authedUserBodyFromRole("administrator")
    testApprovalStatusFxfs(domainSet, userBody, expectedApprovedFxfs, expectedRejectedFxfs, expectedPendingFxfs)
  }

  test("searching as an admin on a moderated & RA-enabled domain (as both search_context & domain), should find the correct set of views for the given approval_status") {
    val raVmDomain = domains(3)
    val allPossibleResults = getAllPossibleResults()
    val approvedFxfs = allPossibleResults.filter(isApproved).map(fxfs)
    val domain3Docs = docs.filter(d => d.socrataId.domainId == 3)
    // approved views are views that pass all 3 types of approval
    val expectedApprovedFxfs = fxfs(domain3Docs.filter(d => approvedFxfs.contains(d.socrataId.datasetId)))
    // rejected views are those rejected by view moderation/DL status or rejected by the context's RA process
    val expectedRejectedFxfs = fxfs(domain3Docs.filter(d => d.isRejectedByParentDomain || d.isVmRejected))
    // pending views are those pending by view moderation/DL status or rejected by the context's RA process
    val expectedPendingFxfs = fxfs(domain3Docs.filter(d => d.isPendingOnParentDomain || d.isVmPending))

    val domainSet = DomainSet(Set(raVmDomain), Some(raVmDomain))
    val userBody = authedUserBodyFromRole("administrator")
    testApprovalStatusFxfs(domainSet, userBody, expectedApprovedFxfs, expectedRejectedFxfs, expectedPendingFxfs)
  }

  test("searching as an admin on an unmoderated domain that federates in data from a moderated domain, should find the correct set of views for the given approval_status") {
    // domain 0 is an unmoderated domain with approved/pending/rejected datalenses
    // domain 1 is a moderated domain
    val unmoderatedDomain = domains(0)
    val moderatedDomain = domains(1)
    val domain0Docs = docs.filter(d => d.socrataId.domainId == 0)
    val domain1Docs = docs.filter(d => d.socrataId.domainId == 1)
    val domain0Datalenses = domain0Docs.filter(d => d.datatype.startsWith("datalens"))
    val allPossibleResults = getAllPossibleResults()
    val approvedFxfs = allPossibleResults.filter(isApproved).map(fxfs)

    // on both 0 and 1, approved views are those that pass all 3 types of approval
    val approvedOn0 = fxfs(domain0Docs.filter(d => approvedFxfs.contains(d.socrataId.datasetId)))
    val approvedOn1 = fxfs(domain1Docs.filter(d => anonymouslyViewableDocIds.contains(d.socrataId.datasetId)))
    val expectedApprovedFxfs = approvedOn0 ++ approvedOn1

    // on 0, b/c it's unmoderated, the only rejected views can be datalenses
    val rejectedOn0 = fxfs(domain0Datalenses.filter(d => d.isVmRejected))
    // on 1, nothing should come back rejected, b/c an admin on domain 0 doesn't have the rights to see rejected views on domain 1
    val rejectedOn1 = List.empty
    // confirm there are rejected views on domain 1 that could have come back:
    domain1Docs.filter(d => d.isVmRejected) shouldNot be('empty)
    val expectedRejectedFxfs = rejectedOn0 ++ rejectedOn1

    // on 0, b/c it's unmoderated, the only pending views can be datalenses
    val pendingOn0 = fxfs(domain0Datalenses.filter(d => d.isVmPending))
    // on 1, nothing should come back pending, b/c an admin on domain 0 doesn't have the rights to see pendings views on domain 1
    val pendingOn1 = List.empty
    // confirm there are pending views on domain 1 that could have come back:
    domain1Docs.filter(d => d.isVmPending) shouldNot be('empty)
    val expectedPendingFxfs = pendingOn0 ++ pendingOn1

    val domainSet = DomainSet(Set(unmoderatedDomain, moderatedDomain), Some(unmoderatedDomain))
    val userBody = authedUserBodyFromRole("administrator")
    testApprovalStatusFxfs(domainSet, userBody, expectedApprovedFxfs, expectedRejectedFxfs, expectedPendingFxfs)
  }

  test("searching as an admin on a moderated domain that federates in data from an unmoderated domain, should find the correct set of views for the given approval_status") {
    // domain 1 is a moderated domain with approved/pending/rejected views
    // domain 0 is an unmoderated domain with approved/pending/rejected datalenses
    val moderatedDomain = domains(1)
    val unmoderatedDomain = domains(0)
    val domain1Docs = docs.filter(d => d.socrataId.domainId == 1)
    val domain0Docs = docs.filter(d => d.socrataId.domainId == 0)
    val allPossibleResults = getAllPossibleResults()
    val approvedFxfs = allPossibleResults.filter(isApproved).map(fxfs)

    // on 1, approved views are those that pass all 3 types of approval
    val approvedOn1 = fxfs(domain1Docs.filter(d => approvedFxfs.contains(d.socrataId.datasetId)))
    // on 0, b/c federating in unmoderated data to a moderated domain removes all derived views, only default views are approved
    val approvedOn0 = fxfs(domain0Docs.filter(d => d.isDefaultView &&
      (anonymouslyViewableDocIds.contains(d.socrataId.datasetId) || d.isSharedOrOwned("cook-mons"))))
    val expectedApprovedFxfs = approvedOn1 ++ approvedOn0

    // on 1, b/c it's moderated, rejected views are just that - rejected views
    val rejectedOn1 = fxfs(domain1Docs.filter(d => d.isVmRejected))
    // on 0, nothing should come back rejected for 2 reasons:
    //   1) b/c an admin on domain 1 doesn't have the rights to see rejected views on domain 0
    //   2) b/c federating in unmoderated data to a moderated domain removes all derived views
    //   note though, that cook-mons could own/share a rejected view on 0, but this isn't the case
    val rejectedOn0 = List.empty
    // confirm there are rejected views on domain 0 that could have come back:
    domain0Docs.filter(d => d.isVmRejected) shouldNot be('empty)
    val expectedRejectedFxfs = rejectedOn1 ++ rejectedOn0

    // on 1, b/c it's moderated, pending views are just that - pending views
    val pendingOn1 = fxfs(domain1Docs.filter(d => d.isVmPending))
    // on 0, nothing should come back pending for 2 reasons:
    //   1) b/c an admin on domain 1 doesn't have the rights to see pending views on domain 0
    //   2) b/c federating in unmoderated data to a moderated domain removes all derived views
    //   note though, that cook-mons could own/share a pending view on 0, but this isn't the case
    val pendingOn0 = List.empty
    // confirm there are pending views on domain 0 that could have come back:
    domain0Docs.filter(d => d.isVmPending) shouldNot be('empty)
    val expectedPendingFxfs = pendingOn1 ++ pendingOn0

    val domainSet = DomainSet(Set(unmoderatedDomain, moderatedDomain), Some(moderatedDomain))
    val userBody = authedUserBodyFromRole("administrator")
    testApprovalStatusFxfs(domainSet, userBody, expectedApprovedFxfs, expectedRejectedFxfs, expectedPendingFxfs)
  }

  test("searching as an admin on an RA-disabled domain that federates in data from an RA-enabled domain, should find the correct set of views for the given approval_status") {
    // domain 0 is an RA-disabled domain
    // domain 2 is an RA-enabled domain
    val raDisabledDomain = domains(0)
    val raEnabledDomain = domains(2)
    val domain0Docs = docs.filter(d => d.socrataId.domainId == 0)
    val domain2Docs = docs.filter(d => d.socrataId.domainId == 2)
    val domain0Datalenses = domain0Docs.filter(d => d.datatype.startsWith("datalens"))
    val allPossibleResults = getAllPossibleResults()
    val approvedFxfs = allPossibleResults.filter(isApproved).map(fxfs)

    // on both 0 and 2, approved views are those that pass all 3 types of approval
    val approvedOn0 = fxfs(domain0Docs.filter(d => approvedFxfs.contains(d.socrataId.datasetId)))
    val approvedOn2 = fxfs(domain2Docs.filter(d => anonymouslyViewableDocIds.contains(d.socrataId.datasetId)))
    val expectedApprovedFxfs = approvedOn0 ++ approvedOn2

    // on 0, b/c it's RA-disabled, the only rejected views can be from 0's domain and since there is no view-moderation in place
    // can only be rejected datalenses
    val rejectedOn0 = fxfs(domain0Datalenses.filter(d => d.isVmRejected))
    // on 2, nothing should come back rejected, b/c an admin on domain 0 doesn't have the rights to see rejected views on domain 2
    val rejectedOn2 = List.empty
    // confirm there are rejected views on domain 2 that could have come back
    domain2Docs.filter(d => d.isRejectedByParentDomain) shouldNot be('empty)
    val expectedRejectedFxfs = rejectedOn0 ++ rejectedOn2

    // on 0, b/c it's RA-disabled, the only pending views can be from 0's domain and since there is no view-moderation in place
    // can only be pending datalenses
    val pendingOn0 = fxfs(domain0Datalenses.filter(d => d.isVmPending))
    // on 1, nothing should come back pending, b/c an admin on domain 0 doesn't have the rights to see pendings views on domain 1
    val pendingOn2 = List.empty
    // confirm there are pending views on domain 1 that could have come back:
    domain2Docs.filter(d => d.isPendingOnParentDomain) shouldNot be('empty)
    val expectedPendingFxfs = pendingOn0 ++ pendingOn2

    val domainSet = DomainSet(Set(raDisabledDomain, raEnabledDomain), Some(raDisabledDomain))
    val userBody = authedUserBodyFromRole("administrator")
    testApprovalStatusFxfs(domainSet, userBody, expectedApprovedFxfs, expectedRejectedFxfs, expectedPendingFxfs)
  }

  test("searching as an admin on an RA-enabled domain that federates in data from an RA-disabled domain, should find the correct set of views for the given approval_status") {
    // domain 2 is an RA-enabled domain
    // domain 0 is an RA-disabled domain
    val raEnabledDomain = domains(2)
    val raDisabledDomain = domains(0)
    val domain2Docs = docs.filter(d => d.socrataId.domainId == 2)
    val domain0Docs = docs.filter(d => d.socrataId.domainId == 0)

    val allPossibleResults = getAllPossibleResults()
    val approvedFxfs = allPossibleResults.filter(isApproved).map(fxfs)

    // on 2, approved views are those that pass all 3 types of approval
    val approvedOn2 = fxfs(domain2Docs.filter(d => approvedFxfs.contains(d.socrataId.datasetId)))
    // on 0, views must be anonymously viewable (to be seen by a domain 2 admin) and pass all 3 types of approval and
    // additionally be approved by domain 2's RA Queue
    val approvedOn0 = fxfs(domain0Docs.filter(d => anonymouslyViewableDocIds.contains(d.socrataId.datasetId) &&
      approvedFxfs.contains(d.socrataId.datasetId) && d.isRaApproved(2)))
    val expectedApprovedFxfs = approvedOn2 ++ approvedOn0

    // on 2, rejected views those rejected by R&A or rejected as datalenses (domain 2 has no view-moderation)
    val rejectedOn2 = fxfs(domain2Docs.filter(d => d.isRejectedByParentDomain || d.isVmRejected))
    // on 0, despite the fact that an admin on domain 2 doesn't have the rights to see rejected views on domain 0
    // there can be views that are approved on 0, but rejected by domain 2's R&A process.
    val rejectedOn0 = fxfs(domain0Docs.filter(d => anonymouslyViewableDocIds.contains(d.socrataId.datasetId) && d.isRaRejected(2)))
    rejectedOn0 shouldNot be('empty)
    val expectedRejectedFxfs = rejectedOn2 ++ rejectedOn0

    // on 2, pending views those pending by R&A or pending as datalenses (domain 2 has no view-moderation)
    val pendingOn2 = fxfs(domain2Docs.filter(d => d.isVmPending))
    // on 0, despite the fact that an admin on domain 2 doesn't have the rights to see pending views on domain 0
    // there can be views that are approved on 0, but pending by domain 2's R&A process.
    // heh, also an admin on 2 can see pending views on 0, if they've been shared the view, as is the case with zeta-0004
    val pendingOn0 = fxfs(domain0Docs.filter(d => (d.isRaPending(2) || d.isVmPending) &&
      (anonymouslyViewableDocIds.contains(d.socrataId.datasetId) || d.isSharedOrOwned("cook-mons"))))
    pendingOn0 shouldNot be('empty)
    val expectedPendingFxfs = pendingOn2 ++ pendingOn0

    val domainSet = DomainSet(Set(raDisabledDomain, raEnabledDomain), Some(raEnabledDomain))
    val userBody = authedUserBodyFromRole("administrator")
    testApprovalStatusFxfs(domainSet, userBody, expectedApprovedFxfs, expectedRejectedFxfs, expectedPendingFxfs)
  }

  test("searching with the 'q' param finds all items from the authenticating domain where q matches the private metadata") {
    val host = domains(0).domainCname
    val privateValue = "Pumas Inc."
    val expectedFxfs = fxfs(docs.filter(d =>
      d.privateCustomerMetadataFlattened.exists(m => m.value == privateValue &&
      d.socrataId.domainId == 0)))
    val userBody = authedUserBodyFromRole("publisher")
    prepareAuthenticatedUser(cookie, host, userBody)
    val params = allDomainsParams ++ Map("q" -> privateValue, "min_should_match" -> "100%").mapValues(Seq(_))
    val res = service.doSearch(params, AuthParams(cookie=Some(cookie)), Some(host), None)
    val actualFxfs = fxfs(res._2)
    actualFxfs should contain theSameElementsAs expectedFxfs

    // confirm there were documents on other domains that were excluded.
    anonymouslyViewableDocs.find(_.socrataId.datasetId == "fxf-1").get.privateCustomerMetadataFlattened.exists(_.value == privateValue
    ) should be(true)
    actualFxfs should not contain theSameElementsAs(List("fxf-1"))
  }

  test("searching with a private metadata k/v pair param finds all items from the authenticating domain with that pair") {
    val host = domains(0).domainCname
    val privateKey = "Secret domain 0 cat organization"
    val privateValue = "Pumas Inc."
    val expectedFxfs = fxfs(docs.filter(d =>
      d.privateCustomerMetadataFlattened.exists(m => m.value == privateValue &&
      d.socrataId.domainId == 0)))
    val userBody = authedUserBodyFromRole("publisher")
    prepareAuthenticatedUser(cookie, host, userBody)
    val params = allDomainsParams ++ Map(privateKey -> Seq(privateValue))
    val res = service.doSearch(params, AuthParams(cookie=Some(cookie)), Some(host), None)
    val actualFxfs = fxfs(res._2)
    actualFxfs should contain theSameElementsAs expectedFxfs

    // confirm there were documents on other domains that were excluded.
    anonymouslyViewableDocs.find(_.socrataId.datasetId == "zeta-0002").get.privateCustomerMetadataFlattened.exists(m =>
      m.value == privateValue && m.key == privateKey) should be(true)
    actualFxfs should not contain theSameElementsAs(List("zeta-0002"))
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
