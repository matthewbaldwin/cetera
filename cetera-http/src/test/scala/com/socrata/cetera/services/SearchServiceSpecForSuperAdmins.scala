package com.socrata.cetera.services

import org.scalatest.{BeforeAndAfterAll, BeforeAndAfterEach, FunSuiteLike, Matchers}

import com.socrata.cetera.auth.AuthParams
import com.socrata.cetera.types.ApprovalStatus
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

  def testApprovalStatus(status: ApprovalStatus) = {
    val expectedFxfs = status match {
      case ApprovalStatus.approved => fxfs(docs.filter(d => d.isApproved(domains(d.socrataId.domainId.toInt))))
      case _ => fxfs(docs.filter(d => d.isPendingOrRejected(domains(d.socrataId.domainId.toInt), status)))
    }

    val host = "annabelle.island.net"
    prepareAuthenticatedUser(cookie, host, superAdminBody)
    val params = allDomainsParams ++ Map("approval_status" -> Seq(status.status))
    val res = service.doSearch(params, AuthParams(cookie=Some(cookie)), Some(host), None)
    val actualFxfs = fxfs(res._2)
    actualFxfs should contain theSameElementsAs expectedFxfs
  }

  test("searching across all domains shows everything from every domain") {
    val expectedFxfs = fxfs(docs)

    val host = "annabelle.island.net"
    prepareAuthenticatedUser(cookie, host, superAdminBody)
    val res = service.doSearch(allDomainsParams, AuthParams(cookie=Some(cookie)), Some(host), None)
    val actualFxfs = fxfs(res._2)

    actualFxfs should contain theSameElementsAs expectedFxfs
  }

  test("searching on a locked domains shows data") {
    val lockedDomain = domains(8).cname
    val params = Map(
      "domains" -> lockedDomain,
      "search_context" -> lockedDomain
    ).mapValues(Seq(_))
    val expectedFxfs = fxfs(docs.filter(d => d.socrataId.domainId == 8))
    prepareAuthenticatedUser(cookie, lockedDomain, superAdminBody)
    val adminRes = service.doSearch(params, AuthParams(cookie = Some(cookie)), Some(lockedDomain), None)._2
    fxfs(adminRes) should contain theSameElementsAs expectedFxfs
  }

  test("searching across all domains with public=true, should find all public views") {
    val expectedFxfs = fxfs(docs.filter(d => d.isPublic))

    val host = "annabelle.island.net"
    prepareAuthenticatedUser(cookie, host, superAdminBody)
    val params = allDomainsParams ++ Map("public" -> Seq("true"))
    val res = service.doSearch(params, AuthParams(cookie=Some(cookie)), Some(host), None)
    val actualFxfs = fxfs(res._2)

    actualFxfs should contain theSameElementsAs expectedFxfs
  }

  test("searching across all domains with public=false, should find all private views") {
    val expectedFxfs = fxfs(docs.filter(d => !d.isPublic))

    val host = "annabelle.island.net"
    prepareAuthenticatedUser(cookie, host, superAdminBody)
    val params = allDomainsParams ++ Map("public" -> Seq("false"))
    val res = service.doSearch(params, AuthParams(cookie=Some(cookie)), Some(host), None)
    val actualFxfs = fxfs(res._2)

    actualFxfs should contain theSameElementsAs expectedFxfs
  }

  test("searching across all domains with published=true, should find all published views") {
    val expectedFxfs = fxfs(docs.filter(d => d.isPublished))

    val host = "annabelle.island.net"
    prepareAuthenticatedUser(cookie, host, superAdminBody)
    val params = allDomainsParams ++ Map("published" -> Seq("true"))
    val res = service.doSearch(params, AuthParams(cookie=Some(cookie)), Some(host), None)
    val actualFxfs = fxfs(res._2)

    actualFxfs should contain theSameElementsAs expectedFxfs
  }

  test("searching across all domains with published=false, should find all unpublished views") {
    val expectedFxfs = fxfs(docs.filter(d => !d.isPublished))

    val host = "annabelle.island.net"
    prepareAuthenticatedUser(cookie, host, superAdminBody)
    val params = allDomainsParams ++ Map("published" -> Seq("false"))
    val res = service.doSearch(params, AuthParams(cookie=Some(cookie)), Some(host), None)
    val actualFxfs = fxfs(res._2)

    actualFxfs should contain theSameElementsAs expectedFxfs
  }

  test("searching across all domains with derived=true, should find all derived views") {
    val expectedFxfs = fxfs(docs.filter(d => !d.isDefaultView))

    val host = "annabelle.island.net"
    prepareAuthenticatedUser(cookie, host, superAdminBody)
    val params = allDomainsParams ++ Map("derived" -> Seq("true"))
    val res = service.doSearch(params, AuthParams(cookie=Some(cookie)), Some(host), None)
    val actualFxfs = fxfs(res._2)

    actualFxfs should contain theSameElementsAs expectedFxfs
  }

  test("searching across all domains with derived=false, should find all default views") {
    val expectedFxfs = fxfs(docs.filter(d => d.isDefaultView))

    val host = "annabelle.island.net"
    prepareAuthenticatedUser(cookie, host, superAdminBody)
    val params = allDomainsParams ++ Map("derived" -> Seq("false"))
    val res = service.doSearch(params, AuthParams(cookie=Some(cookie)), Some(host), None)
    val actualFxfs = fxfs(res._2)

    actualFxfs should contain theSameElementsAs expectedFxfs
  }

  test("searching across all domains with explicity_hidden=true, should find all hidden views") {
    val expectedFxfs = fxfs(docs.filter(d => d.isHiddenFromCatalog))

    val host = "annabelle.island.net"
    prepareAuthenticatedUser(cookie, host, superAdminBody)
    val params = allDomainsParams ++ Map("explicitly_hidden" -> Seq("true"))
    val res = service.doSearch(params, AuthParams(cookie=Some(cookie)), Some(host), None)
    val actualFxfs = fxfs(res._2)

    actualFxfs should contain theSameElementsAs expectedFxfs
  }

  test("searching across all domains with explicitly_hidden=false, should find all not-hidden views") {
    val expectedFxfs = fxfs(docs.filter(d => !d.isHiddenFromCatalog))

    val host = "annabelle.island.net"
    prepareAuthenticatedUser(cookie, host, superAdminBody)
    val params = allDomainsParams ++ Map("explicitly_hidden" -> Seq("false"))
    val res = service.doSearch(params, AuthParams(cookie=Some(cookie)), Some(host), None)
    val actualFxfs = fxfs(res._2)

    actualFxfs should contain theSameElementsAs expectedFxfs
  }

  test("searching across all domains with approval_status=approved should find all approved results") {
    testApprovalStatus(ApprovalStatus.approved)
  }

  test("searching across all domains with approval_status=rejected and no context should find all rejected results") {
    testApprovalStatus(ApprovalStatus.rejected)
  }
  test("searching across all domains with approval_status=pending and no context should find all pending results") {
    testApprovalStatus(ApprovalStatus.pending)
  }

  test("searching with the 'q' param finds all items where q matches the private metadata") {
    val host = domains(0).cname
    val privateValue = "Cheetah Corp."
    val expectedFxfs = fxfs(docs.filter(d => d.privateCustomerMetadataFlattened.exists(m => m.value == privateValue)))
    prepareAuthenticatedUser(cookie, host, superAdminBody)
    val params = allDomainsParams ++ Map("q" -> Seq(privateValue))
    val res = service.doSearch(params, AuthParams(cookie=Some(cookie)), Some(host), None)
    val actualFxfs = fxfs(res._2)
    actualFxfs should contain theSameElementsAs expectedFxfs
  }

  test("searching with a private metadata k/v pair param finds all documents with that pair") {
    val host = domains(0).cname
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
    val host = domains(3).cname
    val expectedFxfs = fxfs(docs.filter(d => d.sharedTo.contains("lil-john")))
    prepareAuthenticatedUser(cookie, host, superAdminBody)
    val params = allDomainsParams ++ Map("shared_to" -> Seq("lil-john"))
    val res = service.doSearch(params, AuthParams(cookie=Some(cookie)), Some(host), None)
    val actualFxfs = fxfs(res._2)
    actualFxfs should contain theSameElementsAs expectedFxfs
  }
}
