package com.socrata.cetera.services

import com.rojoma.json.v3.interpolation._
import com.rojoma.json.v3.io.CompactJsonWriter
import com.socrata.http.server.responses._
import org.mockserver.model.HttpRequest._
import org.mockserver.model.HttpResponse._
import org.scalatest.{BeforeAndAfterAll, BeforeAndAfterEach, FunSuiteLike, Matchers}

import com.socrata.cetera.auth.AuthParams
import com.socrata.cetera.errors.{DomainNotFoundError, MissingRequiredParameterError, UnauthorizedError}
import com.socrata.cetera.handlers.Params
import com.socrata.cetera.response.{CompletionMatch, MatchSpan}
import com.socrata.cetera.search.UserClient
import com.socrata.cetera.types.DomainUser
import com.socrata.cetera.{HeaderAuthorizationKey, HeaderCookieKey, HeaderXSocrataHostKey, TestESData}

class UserAutocompleteServiceSpec extends FunSuiteLike with Matchers with TestESData
  with BeforeAndAfterAll with BeforeAndAfterEach {

  val userClient = new UserClient(client, testSuiteName)
  val userAutocompleteService = new UserAutocompleteService(userClient, domainClient, coreClient)

  override val cookie = "Traditional = WASD"
  val basicAuth = "Basic cHJvZmVzc29yeDpjZXJlYnJvNGxpZmU="
  val oAuth = "OAuth 123456789"
  val context = domains(0)
  val host = context.domainCname
  val adminUserBody = j"""
    {
      "id" : "boo-bear",
      "roleName" : "headBear",
      "rights" : [ "steal_honey", "scare_tourists"],
      "flags" : [ "admin" ]
    }"""

  override protected def beforeAll(): Unit = bootstrapData()

  override def beforeEach(): Unit = mockServer.reset()

  override protected def afterAll(): Unit = {
    removeBootstrapData()
    client.close()
    mockServer.stop(true)
    httpClient.close()
  }

  test("search without authentication throws an unauthorizedError") {
    intercept[UnauthorizedError] {
      userAutocompleteService.doSearch(Map.empty, AuthParams(), None, None)
    }
  }

  test("search with cookie, but no socrata host throws an unauthorizedError") {
    intercept[UnauthorizedError] {
      userAutocompleteService.doSearch(Map.empty, AuthParams(cookie=Some(cookie)), None, None)
    }
  }

  test("search with basic auth, but no socrata host throws an unauthorizedError") {
    val basicAuth = "Basic cHJvZmVzc29yeDpjZXJlYnJvNGxpZmU="
    intercept[UnauthorizedError] {
      userAutocompleteService.doSearch(Map.empty, AuthParams(basicAuth=Some(basicAuth)), None, None)
    }
  }

  test("search with oauth, but no socrata host throws an unauthorizedError") {
    val oAuth = "OAuth cHJvZmVzc29yeDpjZXJlYnJvNGxpZmU="
    intercept[UnauthorizedError] {
      userAutocompleteService.doSearch(Map.empty, AuthParams(oAuth=Some(oAuth)), None, None)
    }
  }

  test("search with authentication but without authorization throws an unauthorizedError") {
    val userBody =
      j"""{
        "id" : "boo-bear",
        "rights" : [ "steal_honey", "scare_tourists"]
        }"""
    val expectedRequest = request()
      .withMethod("GET")
      .withPath("/users.json")
      .withHeader(HeaderXSocrataHostKey, host)
      .withHeader(HeaderCookieKey, cookie)

    mockServer.when(
      expectedRequest
    ).respond(
      response()
        .withStatusCode(200)
        .withHeader("Content-Type", "application/json; charset=utf-8")
        .withBody(CompactJsonWriter.toString(userBody))
    )

    intercept[UnauthorizedError] {
      userAutocompleteService.doSearch(Map(Params.q -> Seq("foo")), AuthParams(cookie=Some(cookie)), Some(host), None)
    }
  }

  test("search for a domain that doesn't exist throws a DomainNotFoundError") {
    val expectedRequest = request()
      .withMethod("GET")
      .withPath("/users.json")
      .withHeader(HeaderXSocrataHostKey, host)
      .withHeader(HeaderCookieKey, cookie)
    mockServer.when(
      expectedRequest
    ).respond(
      response()
        .withStatusCode(200)
        .withHeader("Content-Type", "application/json; charset=utf-8")
        .withBody(CompactJsonWriter.toString(adminUserBody))
    )

    intercept[DomainNotFoundError] {
      val params = Map(Params.domain -> "bad-domain.com").mapValues(Seq(_))
      userAutocompleteService.doSearch(params, AuthParams(cookie=Some(cookie)), Some(host), None)
    }
  }

  test("search with no query parameter raises an exception") {
    val expectedRequest = request()
      .withMethod("GET")
      .withPath("/users.json")
      .withHeader(HeaderXSocrataHostKey, host)
      .withHeader(HeaderCookieKey, cookie)
    mockServer.when(
      expectedRequest
    ).respond(
      response()
        .withStatusCode(200)
        .withHeader("Content-Type", "application/json; charset=utf-8")
        .withBody(CompactJsonWriter.toString(adminUserBody))
    )

    intercept[MissingRequiredParameterError] {
      userAutocompleteService.doSearch(Map.empty, AuthParams(cookie=Some(cookie)), Some(host), None)
    }
  }

  test("search with a well-formed query that doesn't match any user emails or screen names should return no results") {
    val expectedRequest = request()
      .withMethod("GET")
      .withPath("/users.json")
      .withHeader(HeaderXSocrataHostKey, host)
    mockServer.when(
      expectedRequest
    ).respond(
      response()
        .withStatusCode(200)
        .withHeader("Content-Type", "application/json; charset=utf-8")
        .withBody(CompactJsonWriter.toString(adminUserBody))
    )    

    val params = Map(Params.q -> "a").mapValues(Seq(_))
    val (status, results, _, _) = userAutocompleteService.doSearch(params, AuthParams(cookie=Some(cookie)), Some(host), None)
    mockServer.verify(expectedRequest)
    status should be(OK)

    results.results.headOption should be(None)
  }

  test("search with a well-formed query that matches some users should produce the expected results") {
    val expectedRequest = request()
      .withMethod("GET")
      .withPath("/users.json")
      .withHeader(HeaderXSocrataHostKey, host)
    mockServer.when(
      expectedRequest
    ).respond(
      response()
        .withStatusCode(200)
        .withHeader("Content-Type", "application/json; charset=utf-8")
        .withBody(CompactJsonWriter.toString(adminUserBody))
    )    

    val params = Map(Params.q -> "dark.star").mapValues(Seq(_))
    val (status, results, _, _) = userAutocompleteService.doSearch(params, AuthParams(cookie=Some(cookie)), Some(host), None)
    mockServer.verify(expectedRequest)
    status should be(OK)

    results.results.headOption should be('defined)
    val expectedFirstUser = DomainUser(context, users(2))
    results.results.head.user should be(expectedFirstUser)

    val expectedMatches = List(
      CompletionMatch("screen_name", List(MatchSpan(0, 4), MatchSpan(5, 4))),
      CompletionMatch("email", List(MatchSpan(0, 4), MatchSpan(5, 4))))

    results.results.head.matches should contain theSameElementsInOrderAs(expectedMatches)
  }
}
