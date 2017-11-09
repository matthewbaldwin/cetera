package com.socrata.cetera.services

import com.rojoma.json.v3.ast.JValue
import org.scalatest.{BeforeAndAfterAll, BeforeAndAfterEach, FunSuiteLike, Matchers}

import com.socrata.cetera.auth.AuthParams
import com.socrata.cetera.handlers.Params
import com.socrata.cetera.types.{ApprovalStatus, Document, Domain, DomainSet}
import com.socrata.cetera.{response => _, _}

class SearchServiceSpecForUsersWithAllTheRights
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

  def allRights = List("view_others_datasets", "view_story", "edit_others_datasets", "edit_others_stories", "manage_users")
  val cookieMonsUserBody = authedUserBodyWithRoleAndRights(allRights)
  val ownerOfNothingUserBody = authedUserBodyWithRoleAndRights(allRights, "user-but-not-owner")

  private def testApprovalSingleDomain(domain: Domain) = {
    val domDocs = docsForDomain(domain)
    val expectedApprovedFxfs = fxfs(domDocs.filter(d => d.isApproved(domain)))
    val expectedRejectedFxfs = fxfs(domDocs.filter(d => d.isPendingOrRejected(domain, ApprovalStatus.rejected)))
    val expectedPendingFxfs = fxfs(domDocs.filter(d => d.isPendingOrRejected(domain, ApprovalStatus.pending)))

    val host = domain.cname
    prepareAuthenticatedUser(cookie, host, cookieMonsUserBody)

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

  def testApprovalFederatedDomains(context: Domain, federation: Domain,
      expectedApprovals: Option[List[String]] = None,
      expectedRejections: Option[List[String]] = None,
      expectedPending: Option[List[String]] = None) = {
    val contextDocs = docsForDomain(context)
    val federationDocs = docsForDomain(federation)

    val expectedApprovedFxfs = expectedApprovals.getOrElse(fxfs(
      contextDocs.filter(d => d.isApproved(context)) ++
      federationDocs.filter(d => anonymouslyViewableDocIds.contains(d.socrataId.datasetId))))

    // the federation relationship simply will not bring in rejected or pending views from federated sites
    val expectedRejectedFxfs = expectedRejections.getOrElse(fxfs(
      contextDocs.filter(d => d.isPendingOrRejected(context, ApprovalStatus.rejected))))
    val expectedPendingFxfs = expectedPending.getOrElse(fxfs(
      contextDocs.filter(d => d.isPendingOrRejected(context, ApprovalStatus.pending))))

    prepareAuthenticatedUser(cookie, context.cname, cookieMonsUserBody)
    val params = Map("search_context" -> Seq(context.cname), "domains" -> Seq(s"${context.cname},${federation.cname}"))
    val actualApprovedFxfs = fxfs(service.doSearch(params ++ Map("approval_status" -> Seq("approved")),
      AuthParams(cookie=Some(cookie)), Some(context.cname), None)._2)
    val actualRejectedFxfs = fxfs(service.doSearch(params ++ Map("approval_status" -> Seq("rejected")),
      AuthParams(cookie=Some(cookie)), Some(context.cname), None)._2)
    val actualPendingFxfs = fxfs(service.doSearch(params ++ Map("approval_status" -> Seq("pending")),
      AuthParams(cookie=Some(cookie)), Some(context.cname), None)._2)

    actualApprovedFxfs should contain theSameElementsAs expectedApprovedFxfs
    actualRejectedFxfs should contain theSameElementsAs expectedRejectedFxfs
    actualPendingFxfs should contain theSameElementsAs expectedPendingFxfs
  }

  def testVisibilitySingleDomain(dom: Domain) = {
    prepareAuthenticatedUser(cookie, dom.cname, cookieMonsUserBody)
    val params = Map("domains" -> dom.cname).mapValues(Seq(_))
    val domDocs = docsForDomain(dom)
    val (expectedOpenDocs, expectedInternalDocs) = domDocs.partition(d => anonymouslyViewableDocIds.contains(d.socrataId.datasetId))

    val openParams = Map("domains" -> dom.cname, "visibility" -> "open").mapValues(Seq(_))
    val internalParams = Map("domains" -> dom.cname, "visibility" -> "internal").mapValues(Seq(_))
    val actualOpenFxfs = fxfs(service.doSearch(openParams, AuthParams(cookie=Some(cookie)), Some(dom.cname), None)._2)
    val actualInternalFxfs = fxfs(service.doSearch(internalParams, AuthParams(cookie=Some(cookie)), Some(dom.cname), None)._2)

    actualOpenFxfs shouldNot be('empty)
    actualInternalFxfs shouldNot be('empty)

    actualOpenFxfs should contain theSameElementsAs fxfs(expectedOpenDocs)
    actualInternalFxfs should contain theSameElementsAs fxfs(expectedInternalDocs)
  }

  test("searching across all domains finds " +
    "a) everything from their domain " +
    "b) anonymously visible views from unlocked domains " +
    "c) views they own/share ") {
    val raDisabledDomain = domains(0)
    val withinDomain = docs.filter(d => d.socrataId.domainId == raDisabledDomain.id).map(d => d.socrataId.datasetId)
    val ownedByCookieMonster = docs.collect{ case d: Document if d.owner.id == "cook-mons" => d.socrataId.datasetId }
    ownedByCookieMonster should be(List("d0-v6"))
    val sharedToCookieMonster = docs.collect{ case d: Document if d.sharedTo.contains("cook-mons") => d.socrataId.datasetId }
    sharedToCookieMonster should be(List("d0-v5"))
    val expectedFxfs = (withinDomain ++ ownedByCookieMonster ++ sharedToCookieMonster ++ anonymouslyViewableDocIds).distinct

    val host = raDisabledDomain.cname
    prepareAuthenticatedUser(cookie, host, cookieMonsUserBody)
    val res = service.doSearch(allDomainsParams, AuthParams(cookie = Some(cookie)), Some(host), None)
    fxfs(res._2) should contain theSameElementsAs expectedFxfs
  }

  test("searching on a locked domains shows data from that domain") {
    val lockedDomain = domains(8).cname
      val params = Map(
        "domains" -> lockedDomain,
        "search_context" -> lockedDomain
      ).mapValues(Seq(_))
    val expectedFxfs = fxfs(docs.filter(d => d.socrataId.domainId == 8))

    prepareAuthenticatedUser(cookie, lockedDomain, cookieMonsUserBody)
    val res = service.doSearch(params, AuthParams(cookie = Some(cookie)), Some(lockedDomain), None)._2
    fxfs(res) should contain theSameElementsAs expectedFxfs
  }

  test("searching for assets owned by another user (robin-hood) when the logged-in user is cook-mons returns " +
    " a) any views that robin-hood owns on cook-mons' domain " +
    " b) anonymously viewable views that robin-hood owns on any unlocked domain ") {
    // this has cook-mons (admin on domain 0) checking up on robin-hood
    val authenticatingDomain = domains(0).cname
    val params = Map("for_user" -> Seq("robin-hood"))
    prepareAuthenticatedUser(cookie, authenticatingDomain, cookieMonsUserBody)

    val ownedByRobin = docs.collect{ case d: Document if d.owner.id == "robin-hood" => d.socrataId.datasetId }.toSet
    val onDomain0 = docs.collect{ case d: Document if d.socrataId.domainId == 0 => d.socrataId.datasetId}.toSet
    val anonymouslyViewable = anonymouslyViewableDocIds.toSet
    val expectedFxfs = ownedByRobin & (onDomain0 ++ anonymouslyViewable)

    val res = service.doSearch(params, AuthParams(cookie = Some(cookie)), Some(authenticatingDomain), None)

    // confirm that robin has documents on other domains that are being excluded
    expectedFxfs.size < ownedByRobin.size should be(true)
    fxfs(res._2) should contain theSameElementsAs expectedFxfs
  }

  test("hidden documents on the user's domain should be visible") {
    val host = domains(0).cname
    val hiddenDoc = docs.filter(d => d.socrataId.domainId == 0 && !d.isHiddenFromCatalog).headOption.get
    prepareAuthenticatedUser(cookie, host, ownerOfNothingUserBody)

    val (_, res, _, _) = service.doSearch(Map(
      Params.ids -> hiddenDoc.socrataId.datasetId
    ).mapValues(Seq(_)), AuthParams(cookie = Some(cookie)), Some(host), None)
    val actualFxfs = fxfs(res)
    actualFxfs(0) should be(hiddenDoc.socrataId.datasetId)
  }

  test("hidden documents on a domain other than the users should be hidden") {
    val host = domains(1).cname
    val hiddenDoc = docs.filter(d => d.socrataId.domainId == 0 && d.isHiddenFromCatalog).headOption.get
    prepareAuthenticatedUser(cookie, host, ownerOfNothingUserBody)

    val (_, res, _, _) = service.doSearch(Map(
      Params.ids -> hiddenDoc.socrataId.datasetId
    ).mapValues(Seq(_)), AuthParams(cookie = Some(cookie)), Some(host), None)
    val actualFxfs = fxfs(res)
    actualFxfs should be('empty)
  }

  test("private documents on the user's domain should be visible") {
    val host = domains(2).cname
    val privateDoc = docs.filter(d => d.socrataId.domainId == 2 && !d.isPublic).headOption.get
    prepareAuthenticatedUser(cookie, host, ownerOfNothingUserBody)

    val (_, res, _, _) = service.doSearch(Map(
      Params.ids -> privateDoc.socrataId.datasetId
    ).mapValues(Seq(_)), AuthParams(cookie = Some(cookie)), Some(host), None)
    val actualFxfs = fxfs(res)
    actualFxfs(0) should be(privateDoc.socrataId.datasetId)
  }

  test("private documents on a domain other than the users should be hidden") {
    val host = domains(4).cname
    val privateDoc = docs.filter(d => d.socrataId.domainId == 2 && !d.isPublic).headOption.get
    prepareAuthenticatedUser(cookie, host, ownerOfNothingUserBody)

    val (_, res, _, _) = service.doSearch(Map(
      Params.ids -> privateDoc.socrataId.datasetId
    ).mapValues(Seq(_)), AuthParams(cookie = Some(cookie)), Some(host), None)
    val actualFxfs = fxfs(res)
    actualFxfs should be('empty)
  }

  test("unpublished documents on the user's domain should be visible") {
    val host = domains(0).cname
    val unpublishedDoc = docs.filter(d => d.socrataId.domainId == 0 && !d.isPublished).headOption.get
    prepareAuthenticatedUser(cookie, host, ownerOfNothingUserBody)

    val (_, res, _, _) = service.doSearch(Map(
      Params.ids -> unpublishedDoc.socrataId.datasetId
    ).mapValues(Seq(_)), AuthParams(cookie = Some(cookie)), Some(host), None)
    val actualFxfs = fxfs(res)
    actualFxfs(0) should be(unpublishedDoc.socrataId.datasetId)
  }

  test("unpublished documents on a domain other than the users should be hidden") {
    val host = domains(6).cname
    val unpublishedDoc = docs.filter(d => d.socrataId.domainId == 0 && !d.isPublished).headOption.get
    prepareAuthenticatedUser(cookie, host, ownerOfNothingUserBody)

    val (_, res, _, _) = service.doSearch(Map(
      Params.ids -> unpublishedDoc.socrataId.datasetId
    ).mapValues(Seq(_)), AuthParams(cookie = Some(cookie)), Some(host), None)
    val actualFxfs = fxfs(res)
    actualFxfs should be('empty)
  }

  test("searching on a fontana domain should find the correct set of views for the given approval_status") {
    testApprovalSingleDomain(fontanaDomain)
  }

  test("searching on a VM domain should find the correct set of views for the given approval_status") {
    testApprovalSingleDomain(vmDomain)
  }

  test("searching on an RA domain should find the correct set of views for the given approval_status") {
    testApprovalSingleDomain(raDomain)
  }

  test("searching on a VM + RA domain should find the correct set of views for the given approval_status") {
    testApprovalSingleDomain(vmRaDomain)
  }

  // When a fontana domain is the context - everything is easy, b/c no extra context filtering is needed
  test("searching on a fontana domain federating in a fontana domain should find the correct set of views for the given approval_status") {
    testApprovalFederatedDomains(fontanaDomain, domains(9))
  }

  test("searching on a fontana domain federating in a VM domain should find the correct set of views for the given approval_status") {
    testApprovalFederatedDomains(fontanaDomain, vmDomain)
  }

  test("searching on a fontana domain federating in an RA domain should find the correct set of views for the given approval_status") {
    testApprovalFederatedDomains(fontanaDomain, raDomain)
  }

  test("searching on a fontana domain federating in a VM + RA domain should find the correct set of views for the given approval_status") {
    testApprovalFederatedDomains(fontanaDomain, vmRaDomain)
  }

  // When a VM domain is the context, extra context filtering is needed if the federation is not a fontana domain or
  // if the federation lacks VM
  test("searching on a VM domain federating in a fontana domain should find the correct set of views for the given approval_status") {
    testApprovalFederatedDomains(vmDomain, fontanaDomain)
  }

  test("searching on a VM domain federating in a VM domain should find the correct set of views for the given approval_status") {
    testApprovalFederatedDomains(vmDomain, vmRaDomain)
  }

  test("searching on a VM domain federating in an RA domain should find the correct set of views for the given approval_status") {
    val context = vmDomain
    val federation = raDomain
    val contextDocs = docsForDomain(context)
    val federationDocs = docsForDomain(federation)

    val expectedApprovedFxfs = Some(fxfs(
      contextDocs.filter(d => d.isApproved(context)) ++
      federationDocs.filter(d => anonymouslyViewableDocIds.contains(d.socrataId.datasetId) && d.isDefaultView)
    ))
    testApprovalFederatedDomains(context, federation, expectedApprovedFxfs)
  }

  // When an RA domain is the context, extra context filtering is needed if the federation is not a fontana domain or
  // the federation's view haven't gone through the context's RA queue
  test("searching on an RA domain federating in a fontana domain should find the correct set of views for the given approval_status") {
    testApprovalFederatedDomains(raDomain, fontanaDomain)
  }

  test("searching on an RA domain federating in a VM domain should find the correct set of views for the given approval_status") {
    val context = raDomain
    val federation = vmDomain
    val contextDocs = docsForDomain(context)
    val federationDocs = docsForDomain(federation)

    val expectedApprovedFxfs = Some(fxfs(
      contextDocs.filter(d => d.isApproved(context)) ++
      federationDocs.filter(d => anonymouslyViewableDocIds.contains(d.socrataId.datasetId) && d.isRaApproved(context.id))
    ))
    val expectedRejectedFxfs = Some(fxfs(
      contextDocs.filter(d => d.isPendingOrRejected(context, ApprovalStatus.rejected)) ++
      federationDocs.filter(d => anonymouslyViewableDocIds.contains(d.socrataId.datasetId) && d.isRaRejected(context.id))
    ))
    val expectedPendingFxfs = Some(fxfs(
      contextDocs.filter(d => d.isPendingOrRejected(context, ApprovalStatus.pending)) ++
      federationDocs.filter(d => anonymouslyViewableDocIds.contains(d.socrataId.datasetId) && d.isRaPending(context.id))
    ))
    testApprovalFederatedDomains(context, federation, expectedApprovedFxfs, expectedRejectedFxfs, expectedPendingFxfs)
  }

  test("searching on an RA domain federating in an RA domain should find the correct set of views for the given approval_status") {
    val context = raDomain
    val federation = vmRaDomain
    val contextDocs = docsForDomain(context)
    val federationDocs = docsForDomain(federation)

    val expectedApprovedFxfs = Some(fxfs(
      contextDocs.filter(d => d.isApproved(context)) ++
      federationDocs.filter(d => anonymouslyViewableDocIds.contains(d.socrataId.datasetId) && d.isRaApproved(context.id))
    ))
    val expectedRejectedFxfs = Some(fxfs(
      contextDocs.filter(d => d.isPendingOrRejected(context, ApprovalStatus.rejected)) ++
      federationDocs.filter(d => anonymouslyViewableDocIds.contains(d.socrataId.datasetId) && d.isRaRejected(context.id))
    ))
    val expectedPendingFxfs = Some(fxfs(
      contextDocs.filter(d => d.isPendingOrRejected(context, ApprovalStatus.pending)) ++
      federationDocs.filter(d => anonymouslyViewableDocIds.contains(d.socrataId.datasetId) && d.isRaPending(context.id))
    ))
    testApprovalFederatedDomains(context, federation, expectedApprovedFxfs, expectedRejectedFxfs, expectedPendingFxfs)
  }

  // When a VM + RA domain is the context, extra context filtering for both the defaultness of the view and being in the right RA queue
  test("searching on a VM + RA domain federating in a fontana domain should find the correct set of views for the given approval_status") {
    testApprovalFederatedDomains(vmRaDomain, fontanaDomain)
  }

  test("searching on a VM + RA domain federating in a VM domain should find the correct set of views for the given approval_status") {
    val context = vmRaDomain
    val federation = vmDomain
    val contextDocs = docsForDomain(context)
    val federationDocs = docsForDomain(federation)

    val expectedApprovedFxfs = Some(fxfs(
      contextDocs.filter(d => d.isApproved(context)) ++
      federationDocs.filter(d => anonymouslyViewableDocIds.contains(d.socrataId.datasetId) && d.isRaApproved(context.id))
    ))
    val expectedRejectedFxfs = Some(fxfs(
      contextDocs.filter(d => d.isPendingOrRejected(context, ApprovalStatus.rejected)) ++
      federationDocs.filter(d => anonymouslyViewableDocIds.contains(d.socrataId.datasetId) && d.isRaRejected(context.id))
    ))
    val expectedPendingFxfs = Some(fxfs(
      contextDocs.filter(d => d.isPendingOrRejected(context, ApprovalStatus.pending)) ++
      federationDocs.filter(d => anonymouslyViewableDocIds.contains(d.socrataId.datasetId) && d.isRaPending(context.id))
    ))
    testApprovalFederatedDomains(context, federation, expectedApprovedFxfs, expectedRejectedFxfs, expectedPendingFxfs)
  }

  test("searching on a VM + RA domain federating in an RA domain should find the correct set of views for the given approval_status") {
    val context = vmRaDomain
    val federation = raDomain
    val contextDocs = docsForDomain(context)
    val federationDocs = docsForDomain(federation)

    val expectedApprovedFxfs = Some(fxfs(
      contextDocs.filter(d => d.isApproved(context)) ++
      federationDocs.filter(d => anonymouslyViewableDocIds.contains(d.socrataId.datasetId) && d.isDefaultView && d.isRaApproved(context.id))
    ))
    val expectedRejectedFxfs = Some(fxfs(
      contextDocs.filter(d => d.isPendingOrRejected(context, ApprovalStatus.rejected)) ++
      federationDocs.filter(d => anonymouslyViewableDocIds.contains(d.socrataId.datasetId) && d.isRaRejected(context.id))
    ))
    val expectedPendingFxfs = Some(fxfs(
      contextDocs.filter(d => d.isPendingOrRejected(context, ApprovalStatus.pending)) ++
      federationDocs.filter(d => anonymouslyViewableDocIds.contains(d.socrataId.datasetId) && d.isRaPending(context.id))
    ))
    testApprovalFederatedDomains(context, federation, expectedApprovedFxfs, expectedRejectedFxfs, expectedPendingFxfs)
  }

  test("searching with a private metadata k/v pair param finds all items from the authenticating domain with that pair") {
    val host = domains(0).cname
    val privateKey = "Secret domain 0 cat organization"
    val privateValue = "Pumas Inc."
    val expectedFxfs = fxfs(docs.filter(d =>
      d.privateCustomerMetadataFlattened.exists(m => m.value == privateValue &&
      d.socrataId.domainId == 0)))
    prepareAuthenticatedUser(cookie, host, cookieMonsUserBody)
    val params = allDomainsParams ++ Map(privateKey -> Seq(privateValue))
    val res = service.doSearch(params, AuthParams(cookie=Some(cookie)), Some(host), None)
    val actualFxfs = fxfs(res._2)
    actualFxfs should contain theSameElementsAs expectedFxfs

    // confirm there were documents on other domains that were excluded.
    anonymouslyViewableDocs.find(_.socrataId.datasetId == "d3-v4").get.privateCustomerMetadataFlattened.exists(m =>
      m.value == privateValue && m.key == privateKey) should be(true)
    actualFxfs should not contain theSameElementsAs(List("d3-v4"))
  }

  test("searching on a fontana domain should find the correct set of views for the given visibility") {
    testVisibilitySingleDomain(fontanaDomain)
  }

  test("searching on a VM domain should find the correct set of views for the given visibility") {
    testVisibilitySingleDomain(vmDomain)
  }

  test("searching on an RA domain should find the correct set of views for the given visibility") {
    testVisibilitySingleDomain(raDomain)
  }

  test("searching on a VM + RA domain should find the correct set of views for the given visibility") {
    testVisibilitySingleDomain(vmRaDomain)
  }
}
