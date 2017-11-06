package com.socrata.cetera.services

import com.rojoma.json.v3.ast.JValue
import org.scalatest.{BeforeAndAfterAll, BeforeAndAfterEach, FunSuiteLike, Matchers}

import com.socrata.cetera.TestESData
import com.socrata.cetera.auth.AuthParams
import com.socrata.cetera.handlers.Params
import com.socrata.cetera.types.{Document, DomainSet}


class SearchServiceSpecForUsersWithStoriesRights
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

  def storiesRights = List("view_story", "edit_others_stories")
  val cookieMonsUserBody = authedUserBodyWithRoleAndRights(storiesRights)
  val ownerOfNothingUserBody = authedUserBodyWithRoleAndRights(storiesRights, "user-but-not-owner")

  test("searching across all domains finds " +
    "a) anonymously visible view from their domain " +
    "b) all stories from their domain " +
    "c) anonymously visible views from unlocked domains " +
    "d) views they own/share ") {
    val dom = domains(0)
    val storiesWithinDomain = docs.filter(d => d.socrataId.domainId == dom.domainId && d.isStory).map(d => d.socrataId.datasetId)
    val internalNonStoriesWithinDomain = internalDocs.filter(d => d.socrataId.domainId == dom.domainId && !d.isStory).map(d => d.socrataId.datasetId)
    val ownedByCookieMonster = docs.filter(d => d.owner.id == "cook-mons").map(d => d.socrataId.datasetId)
    ownedByCookieMonster should be(List("d0-v6"))
    val sharedToCookieMonster = docs.filter(d => d.sharedTo.contains("cook-mons")).map(d => d.socrataId.datasetId)
    sharedToCookieMonster should be(List("d0-v5"))
    val expectedFxfs = (storiesWithinDomain ++ ownedByCookieMonster ++ sharedToCookieMonster ++ anonymouslyViewableDocIds).distinct
    val internalNonStoriesThatCookieMonsCantSee = internalNonStoriesWithinDomain.toSet -- sharedToCookieMonster.toSet -- ownedByCookieMonster.toSet

    val host = dom.domainCname
    prepareAuthenticatedUser(cookie, host, cookieMonsUserBody)
    val res = service.doSearch(allDomainsParams, AuthParams(cookie = Some(cookie)), Some(host), None)
    val resFxfs = fxfs(res._2)

    // confirm there were internal non-stories on the domain
    internalNonStoriesThatCookieMonsCantSee shouldNot be('empty)
    // confirm the internal non-stories that cookie-mons neither owns or shares were not returned
    internalNonStoriesThatCookieMonsCantSee.foreach(resFxfs should not contain _)
    // confirm the internal non-stories that cookie-mons owns/shares are returned
    internalNonStoriesWithinDomain.toSet.intersect(sharedToCookieMonster.toSet) shouldNot be('empty)
    internalNonStoriesWithinDomain.toSet.intersect(ownedByCookieMonster.toSet) shouldNot be('empty)
    // confirm the results contain the expected 4x4s
    resFxfs should contain theSameElementsAs expectedFxfs
  }

  test("searching for assets owned by another user (robin-hood) when the logged-in user is cook-mons returns " +
    " a) any stories that robin-hood owns on cook-mons' domain " +
    " b) anonymously viewable views that robin-hood owns on any unlocked domain ") {
    // this has cook-mons (admin on domain 0) checking up on robin-hood
    val authenticatingDomain = domains(0).domainCname
    val params = Map("for_user" -> Seq("robin-hood"))
    prepareAuthenticatedUser(cookie, authenticatingDomain, cookieMonsUserBody)

    val ownedByRobinAndAnonymouslyViewable =
      anonymouslyViewableDocs.collect{ case d: Document if d.owner.id == "robin-hood" => d.socrataId.datasetId }.toSet
    val storiesOwnedByRobinOnDomain0 =
      docs.collect{ case d: Document if d.socrataId.domainId == 0 && d.owner.id == "robin-hood" && d.isStory => d.socrataId.datasetId}.toSet
    val expectedFxfs = ownedByRobinAndAnonymouslyViewable ++ storiesOwnedByRobinOnDomain0

    val res = service.doSearch(params, AuthParams(cookie = Some(cookie)), Some(authenticatingDomain), None)

    // confirm the stories weren't returned because they were anonymously viewable
    anonymouslyViewableDocIds shouldNot contain(storiesOwnedByRobinOnDomain0)
    fxfs(res._2) should contain theSameElementsAs expectedFxfs
  }

  test("hidden stories on the user's domain should be visible") {
    val host = domains(0).domainCname
    val hiddenDoc = docs.filter(d => d.socrataId.domainId == 0 && d.isHiddenFromCatalog && d.isStory).headOption.get
    prepareAuthenticatedUser(cookie, host, ownerOfNothingUserBody)

    val (_, res, _, _) = service.doSearch(Map(
      Params.ids -> hiddenDoc.socrataId.datasetId
    ).mapValues(Seq(_)), AuthParams(cookie = Some(cookie)), Some(host), None)
    val actualFxfs = fxfs(res)
    actualFxfs(0) should be(hiddenDoc.socrataId.datasetId)
  }

  test("hidden non-stories on the user's domain should be hidden") {
    val host = domains(0).domainCname
    val hiddenDoc = docs.filter(d => d.socrataId.domainId == 0 && d.isHiddenFromCatalog && !d.isStory).headOption.get
    prepareAuthenticatedUser(cookie, host, ownerOfNothingUserBody)

    val (_, res, _, _) = service.doSearch(Map(
      Params.ids -> hiddenDoc.socrataId.datasetId
    ).mapValues(Seq(_)), AuthParams(cookie = Some(cookie)), Some(host), None)
    val actualFxfs = fxfs(res)
    actualFxfs should be('empty)
  }

  test("private stories on the user's domain should be visible") {
    val host = domains(0).domainCname
    val privateDoc = docs.filter(d => d.socrataId.domainId == 0 && !d.isPublic && d.isStory).headOption.get
    prepareAuthenticatedUser(cookie, host, ownerOfNothingUserBody)

    val (_, res, _, _) = service.doSearch(Map(
      Params.ids -> privateDoc.socrataId.datasetId
    ).mapValues(Seq(_)), AuthParams(cookie = Some(cookie)), Some(host), None)
    val actualFxfs = fxfs(res)
    actualFxfs(0) should be(privateDoc.socrataId.datasetId)
  }

  test("private non-stories on the user's domain should be hidden") {
    val host = domains(2).domainCname
    val privateDoc = docs.filter(d => d.socrataId.domainId == 2 && !d.isPublic && !d.isStory).headOption.get
    prepareAuthenticatedUser(cookie, host, ownerOfNothingUserBody)

    val (_, res, _, _) = service.doSearch(Map(
      Params.ids -> privateDoc.socrataId.datasetId
    ).mapValues(Seq(_)), AuthParams(cookie = Some(cookie)), Some(host), None)
    val actualFxfs = fxfs(res)
    actualFxfs should be('empty)
  }

  test("unpublished stories on the user's domain should be visible") {
    val host = domains(0).domainCname
    val unpublishedDoc = docs.filter(d => d.socrataId.domainId == 0 && !d.isPublished && d.isStory).headOption.get
    prepareAuthenticatedUser(cookie, host, ownerOfNothingUserBody)

    val (_, res, _, _) = service.doSearch(Map(
      Params.ids -> unpublishedDoc.socrataId.datasetId
    ).mapValues(Seq(_)), AuthParams(cookie = Some(cookie)), Some(host), None)
    val actualFxfs = fxfs(res)
    actualFxfs(0) should be(unpublishedDoc.socrataId.datasetId)
  }

  test("unpublished non-stories on the user's domain should be hidden") {
    val host = domains(0).domainCname
    val unpublishedDoc = docs.filter(d => d.socrataId.domainId == 0 && !d.isPublished && !d.isStory).headOption.get
    prepareAuthenticatedUser(cookie, host, ownerOfNothingUserBody)

    val (_, res, _, _) = service.doSearch(Map(
      Params.ids -> unpublishedDoc.socrataId.datasetId
    ).mapValues(Seq(_)), AuthParams(cookie = Some(cookie)), Some(host), None)
    val actualFxfs = fxfs(res)
    actualFxfs should be('empty)
  }
}

