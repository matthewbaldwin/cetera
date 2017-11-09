package com.socrata.cetera.services

import com.rojoma.json.v3.interpolation._
import org.scalatest.{BeforeAndAfterAll, BeforeAndAfterEach, FunSuiteLike, Matchers}

import com.socrata.cetera.auth.AuthParams
import com.socrata.cetera.types.{ApprovalStatus, Document, Domain}
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

  private def testApprovalSingleDomain(domain: Domain, userId: String) = {
    val visibleDomainsDocs = docs.filter(d => isVisibleToUser(d, userId) && d.socrataId.domainId.toInt == domain.id)
    val expectedApprovedFxfs = fxfs(visibleDomainsDocs.filter(d => d.isApproved(domain)))
    val expectedRejectedFxfs = fxfs(visibleDomainsDocs.filter(d => d.isPendingOrRejected(domain, ApprovalStatus.rejected)))
    val expectedPendingFxfs = fxfs(visibleDomainsDocs.filter(d => d.isPendingOrRejected(domain, ApprovalStatus.pending)))

    val host = domain.cname
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

  def testApprovalFederatedDomains(context: Domain, federation: Domain, userId: String,
      expectedApprovals: Option[List[String]] = None,
      expectedRejections: Option[List[String]] = None,
      expectedPending: Option[List[String]] = None) = {
    val contextDocs = docsForDomain(context)
    val federationDocs = docsForDomain(federation)

    val expectedApprovedFxfs = expectedApprovals.getOrElse(fxfs(
      contextDocs.filter(d => isVisibleToUser(d, userId) && d.isApproved(context)) ++
      federationDocs.filter(d => anonymouslyViewableDocIds.contains(d.socrataId.datasetId))))

    // the federation relationship simply will not bring in rejected or pending views from federated sites
    val expectedRejectedFxfs = expectedRejections.getOrElse(fxfs(
      contextDocs.filter(d => isVisibleToUser(d, userId) && d.isPendingOrRejected(context, ApprovalStatus.rejected))))
    val expectedPendingFxfs = expectedPending.getOrElse(fxfs(
      contextDocs.filter(d => isVisibleToUser(d, userId) && d.isPendingOrRejected(context, ApprovalStatus.pending))))

    prepareAuthenticatedUser(cookie, context.cname, authedUserBodyWithoutRoleOrRights(userId))
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

  def testApprovalMultipleDomains(domA: Domain, domB: Domain, userId: String) = {
    val domADocs = docsForDomain(domA)
    val domBDocs = docsForDomain(domB)

    // without a search context and implied federation, we expect users to be able to find all of the views the have
    // access to
    val expectedApprovedFxfs = fxfs(
      domADocs.filter(d => isVisibleToUser(d, userId) && d.isApproved(domA)) ++
      domBDocs.filter(d => isVisibleToUser(d, userId) && d.isApproved(domB)))
    val expectedRejectedFxfs = fxfs(
      domADocs.filter(d => isVisibleToUser(d, userId) && d.isPendingOrRejected(domA, ApprovalStatus.rejected)) ++
      domBDocs.filter(d => isVisibleToUser(d, userId) && d.isPendingOrRejected(domB, ApprovalStatus.rejected)))
    val expectedPendingFxfs = fxfs(
      domADocs.filter(d => isVisibleToUser(d, userId) && d.isPendingOrRejected(domA, ApprovalStatus.pending)) ++
      domBDocs.filter(d => isVisibleToUser(d, userId) && d.isPendingOrRejected(domB, ApprovalStatus.pending)))

    prepareAuthenticatedUser(cookie, domA.cname, authedUserBodyWithoutRoleOrRights(userId))
    val params = Map("domains" -> Seq(s"${domA.cname},${domB.cname}"))
    val actualApprovedFxfs = fxfs(service.doSearch(params ++ Map("approval_status" -> Seq("approved")),
      AuthParams(cookie=Some(cookie)), Some(domA.cname), None)._2)
    val actualRejectedFxfs = fxfs(service.doSearch(params ++ Map("approval_status" -> Seq("rejected")),
      AuthParams(cookie=Some(cookie)), Some(domA.cname), None)._2)
    val actualPendingFxfs = fxfs(service.doSearch(params ++ Map("approval_status" -> Seq("pending")),
      AuthParams(cookie=Some(cookie)), Some(domA.cname), None)._2)

    actualApprovedFxfs should contain theSameElementsAs expectedApprovedFxfs
    actualRejectedFxfs should contain theSameElementsAs expectedRejectedFxfs
    actualPendingFxfs should contain theSameElementsAs expectedPendingFxfs
  }

  test("searching across all domains finds " +
    "a) anonymously visible views from their domain " +
    "b) anonymously visible views from unlocked domains " +
    "c) views they own/share") {
    val authenticatingDomain = domains(0)
    val ownedByCookieMonster = docs.collect{ case d: Document if d.owner.id == "cook-mons" => d.socrataId.datasetId }
    val sharedToCookieMonster = docs.collect{ case d: Document if d.sharedTo.contains("cook-mons") => d.socrataId.datasetId }
    val expectedFxfs = (ownedByCookieMonster ++ sharedToCookieMonster ++ anonymouslyViewableDocIds).distinct

    val host = authenticatingDomain.cname
    prepareAuthenticatedUser(cookie, host, cookieMonsWithoutRole)
    val res = service.doSearch(allDomainsParams, AuthParams(cookie = Some(cookie)), Some(host), None)
    fxfs(res._2) should contain theSameElementsAs expectedFxfs
  }

  test("searching on a locked domains shows data if the user has a role") {
    val lockedDomain = domains(8).cname
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
    val lockedDomain = domains(8).cname
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
    val authenticatingDomain = domains(0).cname
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
    val authenticatingDomain = domains(0).cname
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
    val authenticatingDomain = domains(0).cname
    val name = "No One"
    prepareAuthenticatedUser(cookie, authenticatingDomain, authedUserBodyWithoutRoleOrRights(name))

    val params = Map("for_user" -> name).mapValues(Seq(_))
    val res = service.doSearch(params, AuthParams(cookie = Some(cookie)), Some(authenticatingDomain), None)._2
    fxfs(res) should be('empty)
  }

  test("searching for assets shared to logged in user returns nothing if nothing is shared") {
    val authenticatingDomain = domains(0).cname
    val name = "No One"
    prepareAuthenticatedUser(cookie, authenticatingDomain, authedUserBodyWithoutRoleOrRights(name))

    val params = Map("shared_to" -> name).mapValues(Seq(_))
    val res = service.doSearch(params, AuthParams(cookie = Some(cookie)), Some(authenticatingDomain), None)._2
    fxfs(res) should be('empty)
  }

  test("searching for assets by sending for_user and shared_to params returns no results") {
    val authenticatingDomain = domains(0).cname
    prepareAuthenticatedUser(cookie, authenticatingDomain, authedUserBodyWithoutRoleOrRights("robin-hood"))

    val params = Map("shared_to" -> "robin-hood", "for_user" -> "robin-hood").mapValues(Seq(_))
    val res = service.doSearch(params, AuthParams(cookie = Some(cookie)), Some(authenticatingDomain), None)._2
    fxfs(res) should be('empty)
  }

  test("searching on a fontana domain should find the correct set of views for the given approval_status") {
    testApprovalSingleDomain(fontanaDomain, "lil-john")
  }

  test("searching on a VM domain should find the correct set of views for the given approval_status") {
    testApprovalSingleDomain(vmDomain, "maid-marian" )
  }

  test("searching on an RA domain should find the correct set of views for the given approval_status") {
    testApprovalSingleDomain(raDomain, "robin-hood")
  }

  test("searching on a VM + RA domain should find the correct set of views for the given approval_status") {
    testApprovalSingleDomain(vmRaDomain, "prince-john")
  }

  // When a fontana domain is the context - everything is easy, b/c no extra context filtering is needed
  test("searching on a fontana domain federating in a fontana domain should find the correct set of views for the given approval_status") {
    testApprovalFederatedDomains(fontanaDomain, domains(9), "cook-mons")
  }

  test("searching on a fontana domain federating in a VM domain should find the correct set of views for the given approval_status") {
    testApprovalFederatedDomains(fontanaDomain, vmDomain, "cook-mons")
  }

  test("searching on a fontana domain federating in an RA domain should find the correct set of views for the given approval_status") {
    testApprovalFederatedDomains(fontanaDomain, raDomain, "cook-mons")
  }

  test("searching on a fontana domain federating in a VM + RA domain should find the correct set of views for the given approval_status") {
    testApprovalFederatedDomains(fontanaDomain, vmRaDomain, "cook-mons")
  }

  // When a VM domain is the context, extra context filtering is needed if the federation is not a fontana domain or
  // if the federation lacks VM
  test("searching on a VM domain federating in a fontana domain should find the correct set of views for the given approval_status") {
    testApprovalFederatedDomains(vmDomain, fontanaDomain, "maid-marian")
  }

  test("searching on a VM domain federating in a VM domain should find the correct set of views for the given approval_status") {
    testApprovalFederatedDomains(vmDomain, vmRaDomain, "maid-marian")
  }

  test("searching on a VM domain federating in an RA domain should find the correct set of views for the given approval_status") {
    val userId = "maid-marian"
    val context = vmDomain
    val federation = raDomain
    val contextDocs = docsForDomain(context)
    val federationDocs = docsForDomain(federation)

    val expectedApprovedFxfs = Some(fxfs(
      contextDocs.filter(d => isVisibleToUser(d, userId) && d.isApproved(context)) ++
      federationDocs.filter(d => anonymouslyViewableDocIds.contains(d.socrataId.datasetId) && d.isDefaultView)
    ))
    testApprovalFederatedDomains(context, federation, userId, expectedApprovedFxfs)
  }

  // When an RA domain is the context, extra context filtering is needed if the federation is not a fontana domain or
  // the federation's view haven't gone through the context's RA queue
  test("searching on an RA domain federating in a fontana domain should find the correct set of views for the given approval_status") {
    testApprovalFederatedDomains(raDomain, fontanaDomain, "robin-hood")
  }

  test("searching on an RA domain federating in a VM domain should find the correct set of views for the given approval_status") {
    val userId = "maid-marian"
    val context = raDomain
    val federation = vmDomain
    val contextDocs = docsForDomain(context)
    val federationDocs = docsForDomain(federation)

    val expectedApprovedFxfs = Some(fxfs(
      contextDocs.filter(d => isVisibleToUser(d, userId) && d.isApproved(context)) ++
      federationDocs.filter(d => anonymouslyViewableDocIds.contains(d.socrataId.datasetId) && d.isRaApproved(context.id))
    ))
    val expectedRejectedFxfs = Some(fxfs(
      contextDocs.filter(d => isVisibleToUser(d, userId) && d.isPendingOrRejected(context, ApprovalStatus.rejected)) ++
      federationDocs.filter(d => anonymouslyViewableDocIds.contains(d.socrataId.datasetId) && d.isPendingOrRejected(context, ApprovalStatus.rejected)))
    )
    val expectedPendingFxfs = Some(fxfs(
      contextDocs.filter(d => isVisibleToUser(d, userId) && d.isPendingOrRejected(context, ApprovalStatus.pending)) ++
      federationDocs.filter(d => anonymouslyViewableDocIds.contains(d.socrataId.datasetId) && d.isPendingOrRejected(context, ApprovalStatus.pending)))
    )
    testApprovalFederatedDomains(context, federation, userId, expectedApprovedFxfs, expectedRejectedFxfs, expectedPendingFxfs)
  }

  test("searching on an RA domain federating in an RA domain should find the correct set of views for the given approval_status") {
    val userId = "robin-hood"
    val context = raDomain
    val federation = vmRaDomain
    val contextDocs = docsForDomain(context)
    val federationDocs = docsForDomain(federation)

    val expectedApprovedFxfs = Some(fxfs(
      contextDocs.filter(d => isVisibleToUser(d, userId) && d.isApproved(context)) ++
      federationDocs.filter(d => anonymouslyViewableDocIds.contains(d.socrataId.datasetId) && d.isRaApproved(context.id))
    ))
    val expectedRejectedFxfs = Some(fxfs(
      contextDocs.filter(d => isVisibleToUser(d, userId) && d.isPendingOrRejected(context, ApprovalStatus.rejected)) ++
        federationDocs.filter(d => anonymouslyViewableDocIds.contains(d.socrataId.datasetId) && d.isPendingOrRejected(context, ApprovalStatus.rejected)))
    )
    val expectedPendingFxfs = Some(fxfs(
      contextDocs.filter(d => isVisibleToUser(d, userId) && d.isPendingOrRejected(context, ApprovalStatus.pending)) ++
        federationDocs.filter(d => anonymouslyViewableDocIds.contains(d.socrataId.datasetId) && d.isPendingOrRejected(context, ApprovalStatus.pending)))
    )
    testApprovalFederatedDomains(context, federation, userId, expectedApprovedFxfs, expectedRejectedFxfs, expectedPendingFxfs)
  }

  // When a VM + RA domain is the context, extra context filtering for both the defaultness of the view and being in the right RA queue
  test("searching on a VM + RA domain federating in a fontana domain should find the correct set of views for the given approval_status") {
    testApprovalFederatedDomains(vmRaDomain, fontanaDomain, "robin-hood")
  }

  test("searching on a VM + RA domain federating in a VM domain should find the correct set of views for the given approval_status") {
    val userId = "robin-hood"
    val context = vmRaDomain
    val federation = vmDomain
    val contextDocs = docsForDomain(context)
    val federationDocs = docsForDomain(federation)

    val expectedApprovedFxfs = Some(fxfs(
      contextDocs.filter(d => isVisibleToUser(d, userId) && d.isApproved(context)) ++
      federationDocs.filter(d => anonymouslyViewableDocIds.contains(d.socrataId.datasetId) && d.isRaApproved(context.id))
    ))
    testApprovalFederatedDomains(context, federation, userId, expectedApprovedFxfs)
  }

  test("searching on a VM + RA domain federating in an RA domain should find the correct set of views for the given approval_status") {
    val userId = "robin-hood"
    val context = vmRaDomain
    val federation = raDomain
    val contextDocs = docsForDomain(context)
    val federationDocs = docsForDomain(federation)

    val expectedApprovedFxfs = Some(fxfs(
      contextDocs.filter(d => isVisibleToUser(d, userId) && d.isApproved(context)) ++
      federationDocs.filter(d => anonymouslyViewableDocIds.contains(d.socrataId.datasetId) && d.isDefaultView && d.isRaApproved(context.id))
    ))
    testApprovalFederatedDomains(context, federation, userId, expectedApprovedFxfs)
  }

  test("searching on a fontana domain and a fontana domain should find the correct set of views for the given approval_status") {
    testApprovalMultipleDomains(fontanaDomain, domains(9), "lil-john")
  }

  test("searching on a fontana domain and a VM domain should find the correct set of views for the given approval_status") {
    testApprovalMultipleDomains(fontanaDomain, vmDomain, "lil-john")
  }

  test("searching on a fontana domain and an RA domain should find the correct set of views for the given approval_status") {
    testApprovalMultipleDomains(fontanaDomain, raDomain, "lil-john")
  }

  test("searching on a fontana domain and a VM + RA domain should find the correct set of views for the given approval_status") {
    testApprovalMultipleDomains(fontanaDomain, vmRaDomain, "lil-john")
  }

  test("searching on a VM domain and a fontana domain should find the correct set of views for the given approval_status") {
    testApprovalMultipleDomains(vmDomain, fontanaDomain, "maid-marian")
  }

  test("searching on a VM domain and a VM domain should find the correct set of views for the given approval_status") {
    testApprovalMultipleDomains(vmDomain, vmRaDomain, "maid-marian")
  }

  test("searching on a VM domain and an RA domain should find the correct set of views for the given approval_status") {
    testApprovalMultipleDomains(vmDomain, raDomain, "maid-marian")
  }

  test("searching on an RA domain and a fontana domain should find the correct set of views for the given approval_status") {
    testApprovalMultipleDomains(raDomain, fontanaDomain, "robin-hood")
  }

  test("searching on an RA domain and a VM domain should find the correct set of views for the given approval_status") {
    testApprovalMultipleDomains(raDomain, vmDomain, "robin-hood")
  }

  test("searching on an RA domain and an RA domain should find the correct set of views for the given approval_status") {
    testApprovalMultipleDomains(raDomain, vmRaDomain, "robin-hood")
  }

  test("searching on a VM + RAdomain and a fontana domain should find the correct set of views for the given approval_status") {
    testApprovalMultipleDomains(vmRaDomain, fontanaDomain, "prince-john")
  }

  test("searching on a VM + RA domain and a VM domain should find the correct set of views for the given approval_status") {
    testApprovalMultipleDomains(vmRaDomain, vmDomain, "prince-john")
  }

  test("searching on a VM + RA domain and an RA domain should find the correct set of views for the given approval_status") {
    testApprovalMultipleDomains(vmRaDomain, raDomain, "prince-john")
  }

  test("searching with the 'q' param finds no items where q matches the private metadata if the user doesn't own/share the doc") {
     val host = domains(0).cname
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
    val host = domains(0).cname
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
    val host = domains(0).cname
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
    val host = domains(0).cname
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
}
