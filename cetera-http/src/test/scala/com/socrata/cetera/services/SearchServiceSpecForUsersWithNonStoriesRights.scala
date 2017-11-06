package com.socrata.cetera.services

import org.scalatest.{BeforeAndAfterAll, BeforeAndAfterEach, FunSuiteLike, Matchers}

import com.socrata.cetera.TestESData
import com.socrata.cetera.auth.AuthParams
import com.socrata.cetera.handlers.Params
import com.socrata.cetera.types.Document


class SearchServiceSpecForUsersWithNonStoriesRights
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

  def nonStoriesRights = List("view_others_datasets", "edit_others_datasets")
  val cookieMonsUserBody = authedUserBodyWithRoleAndRights(nonStoriesRights)
  val ownerOfNothingUserBody = authedUserBodyWithRoleAndRights(nonStoriesRights, "user-but-not-owner")

  test("searching across all domains finds " +
    "a) anonymously visible view from their domain " +
    "b) all non-stories from their domain " +
    "c) anonymously visible views from unlocked domains " +
    "d) views they own/share ") {
    val dom = domains(0)
    val nonStoriesWithinDomain = docs.filter(d => d.socrataId.domainId == dom.domainId && !d.isStory).map(d => d.socrataId.datasetId)
    val internalStoriesWithinDomain = internalDocs.filter(d => d.socrataId.domainId == dom.domainId && d.isStory).map(d => d.socrataId.datasetId)
    val ownedByCookieMonster = docs.collect{ case d: Document if d.owner.id == "cook-mons" => d.socrataId.datasetId }
    ownedByCookieMonster should be(List("d0-v6"))
    val sharedToCookieMonster = docs.collect{ case d: Document if d.sharedTo.contains("cook-mons") => d.socrataId.datasetId }
    sharedToCookieMonster should be(List("d0-v5"))
    val expectedFxfs = (nonStoriesWithinDomain ++ ownedByCookieMonster ++ sharedToCookieMonster ++ anonymouslyViewableDocIds).distinct

    val host = dom.domainCname
    prepareAuthenticatedUser(cookie, host, cookieMonsUserBody)
    val res = service.doSearch(allDomainsParams, AuthParams(cookie = Some(cookie)), Some(host), None)
    val resFxfs = fxfs(res._2)

    // confirm there were internal stories on the domain
    internalStoriesWithinDomain shouldNot be('empty)
    // confirm the internal stories were not returned
    internalStoriesWithinDomain.foreach(resFxfs should not contain _)
    // confirm the results contain the expected 4x4s
    resFxfs should contain theSameElementsAs expectedFxfs
  }

  test("searching for assets owned by another user (robin-hood) when the logged-in user is cook-mons returns " +
    " a) any non-stories that robin-hood owns on cook-mons' domain " +
    " b) anonymously viewable views that robin-hood owns on any unlocked domain ") {
    // this has cook-mons (admin on domain 0) checking up on robin-hood
    val authenticatingDomain = domains(0).domainCname
    val params = Map("for_user" -> Seq("robin-hood"))
    prepareAuthenticatedUser(cookie, authenticatingDomain, cookieMonsUserBody)

    val ownedByRobinAndAnonymouslyViewable =
      anonymouslyViewableDocs.collect{ case d: Document if d.owner.id == "robin-hood" => d.socrataId.datasetId }.toSet
    val nonStoriesOwnedByRobinOnDomain0 =
      docs.collect{ case d: Document if d.socrataId.domainId == 0 && d.owner.id == "robin-hood" && !d.isStory => d.socrataId.datasetId}.toSet
    val expectedFxfs = ownedByRobinAndAnonymouslyViewable ++ nonStoriesOwnedByRobinOnDomain0

    val res = service.doSearch(params, AuthParams(cookie = Some(cookie)), Some(authenticatingDomain), None)
    fxfs(res._2) should contain theSameElementsAs expectedFxfs
  }

  test("hidden non-stories on the user's domain should be visible") {
    val host = domains(0).domainCname
    val hiddenDoc = docs.filter(d => d.socrataId.domainId == 0 && d.isHiddenFromCatalog && !d.isStory).headOption.get
    hiddenDoc.isStory should be(false)
    hiddenDoc.isHiddenFromCatalog should be(true)
    prepareAuthenticatedUser(cookie, host, ownerOfNothingUserBody)

    val (_, res, _, _) = service.doSearch(Map(
      Params.ids -> hiddenDoc.socrataId.datasetId
    ).mapValues(Seq(_)), AuthParams(cookie = Some(cookie)), Some(host), None)
    val actualFxfs = fxfs(res)
    actualFxfs(0) should be(hiddenDoc.socrataId.datasetId)
  }

  test("hidden stories on the user's domain should be hidden") {
    val host = domains(0).domainCname
    val hiddenDoc = docs.filter(d => d.socrataId.domainId == 0 && d.isHiddenFromCatalog && d.isStory).headOption.get
    hiddenDoc.isStory should be(true)
    hiddenDoc.isHiddenFromCatalog should be(true)
    prepareAuthenticatedUser(cookie, host, ownerOfNothingUserBody)

    val (_, res, _, _) = service.doSearch(Map(
      Params.ids -> hiddenDoc.socrataId.datasetId
    ).mapValues(Seq(_)), AuthParams(cookie = Some(cookie)), Some(host), None)
    val actualFxfs = fxfs(res)
    actualFxfs should be('empty)
  }

  test("private non-stories on the user's domain should be visible") {
    val host = domains(2).domainCname
    val privateDoc = docs.filter(d => d.socrataId.domainId == 2 && !d.isPublic && !d.isStory).headOption.get
    privateDoc.isStory should be(false)
    privateDoc.isPublic should be(false)
    prepareAuthenticatedUser(cookie, host, ownerOfNothingUserBody)

    val (_, res, _, _) = service.doSearch(Map(
      Params.ids -> privateDoc.socrataId.datasetId
    ).mapValues(Seq(_)), AuthParams(cookie = Some(cookie)), Some(host), None)
    val actualFxfs = fxfs(res)
    actualFxfs(0) should be(privateDoc.socrataId.datasetId)
  }

  test("private stories on the user's domain should be hidden") {
    val host = domains(0).domainCname
    val privateDoc = docs.filter(d => d.socrataId.domainId == 0 && !d.isPublic && d.isStory).headOption.get
    privateDoc.isStory should be(true)
    privateDoc.isPublic should be(false)
    prepareAuthenticatedUser(cookie, host, ownerOfNothingUserBody)

    val (_, res, _, _) = service.doSearch(Map(
      Params.ids -> privateDoc.socrataId.datasetId
    ).mapValues(Seq(_)), AuthParams(cookie = Some(cookie)), Some(host), None)
    val actualFxfs = fxfs(res)
    actualFxfs should be('empty)
  }

  test("unpublished non-stories on the user's domain should be visible") {
    val host = domains(0).domainCname
    val unpublishedDoc = docs.filter(d => d.socrataId.domainId == 0 && !d.isPublished && !d.isStory).headOption.get
    unpublishedDoc.isStory should be(false)
    unpublishedDoc.isPublished should be(false)
    prepareAuthenticatedUser(cookie, host, ownerOfNothingUserBody)

    val (_, res, _, _) = service.doSearch(Map(
      Params.ids -> unpublishedDoc.socrataId.datasetId
    ).mapValues(Seq(_)), AuthParams(cookie = Some(cookie)), Some(host), None)
    val actualFxfs = fxfs(res)
    actualFxfs(0) should be(unpublishedDoc.socrataId.datasetId)
  }

  test("unpublished stories on the user's domain should be hidden") {
    val host = domains(0).domainCname
    val unpublishedDoc = docs.filter(d => d.socrataId.domainId == 0 && !d.isPublished && d.isStory).headOption.get
    unpublishedDoc.isStory should be(true)
    unpublishedDoc.isPublished should be(false)
    prepareAuthenticatedUser(cookie, host, ownerOfNothingUserBody)

    val (_, res, _, _) = service.doSearch(Map(
      Params.ids -> unpublishedDoc.socrataId.datasetId
    ).mapValues(Seq(_)), AuthParams(cookie = Some(cookie)), Some(host), None)
    val actualFxfs = fxfs(res)
    actualFxfs should be('empty)
  }
}

