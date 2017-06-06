package com.socrata.cetera.auth

import com.rojoma.json.v3.interpolation._
import com.rojoma.json.v3.io.CompactJsonWriter
import org.mockserver.integration.ClientAndServer.startClientAndServer
import org.mockserver.model.HttpRequest.request
import org.mockserver.model.HttpResponse.response
import org.scalatest.{BeforeAndAfterAll, BeforeAndAfterEach, ShouldMatchers, WordSpec}

import com.socrata.cetera._

class CoreClientSpec extends WordSpec with ShouldMatchers with BeforeAndAfterAll with BeforeAndAfterEach {

  val coreTestPort = 8082
  val httpClient = new TestHttpClient()
  val coreClient = new TestCoreClient(httpClient, coreTestPort)
  val mockServer = startClientAndServer(coreTestPort)
  val domain = "forest.com"
  val cookie = "c=cookie"

  override def beforeEach(): Unit = {
    mockServer.reset()
  }

  override def afterAll(): Unit = {
    mockServer.stop(true)
    httpClient.close()
  }

  def setUpMockWithAuthorizedUser(): Unit = {
    val userBody =
      j"""{
        "id" : "boo-bear",
        "screenName" : "boo bear",
        "email" : "boo.bear@forest.com",
        "numberOfFollowers" : 1000,
        "numberOfFriends" : 500,
        "roleName" : "headBear",
        "rights" : [ "steal_honey", "scare_tourists"],
        "flags" : [ "admin" ]
        }"""

    mockServer.when(
      request()
        .withMethod("GET")
        .withPath("/users.json")
        .withHeader(HeaderXSocrataHostKey, domain)
    ).respond(
      response()
        .withStatusCode(200)
        .withHeader("Content-Type", "application/json; charset=utf-8")
        .withBody(CompactJsonWriter.toString(userBody))
    )
  }

  def setUpMockWithUnauthorizeddUser(): Unit = {
    val userBody = j"""{"id" : "lazy-bear"}"""

    mockServer.when(
      request()
        .withMethod("GET")
        .withPath("/users.json")
        .withHeader(HeaderXSocrataHostKey, domain)
    ).respond(
      response()
        .withStatusCode(200)
        .withHeader("Content-Type", "application/json; charset=utf-8")
        .withBody(CompactJsonWriter.toString(userBody))
    )
  }

  "The optionallyAuthenticateuser method" should {
    "return None if no auth params are given" in {
      setUpMockWithAuthorizedUser()
      val userWithoutContext = coreClient.optionallyAuthenticateUser(None, AuthParams(), None)
      val userWithContext = coreClient.optionallyAuthenticateUser(Some(domain), AuthParams(), None)

      userWithoutContext._1 should be(None)
      userWithContext._1 should be(None)
    }

    "return None if no context is given" in {
      setUpMockWithAuthorizedUser()
      val userWithoutAuthParams = coreClient.optionallyAuthenticateUser(None, AuthParams(), None)
      val userWithAuthParams = coreClient.optionallyAuthenticateUser(None, AuthParams(cookie=Some(cookie)), None)

      userWithoutAuthParams._1 should be(None)
      userWithAuthParams._1 should be(None)
    }

    "return Some user if both a valid context and an auth with a valid cookie are given" in {
      setUpMockWithAuthorizedUser()
      val (user, _) = coreClient.optionallyAuthenticateUser(Some(domain), AuthParams(cookie=Some(cookie)), None)

      user should be('defined)
      user.get should have('id ("boo-bear"))
    }
  }

  "The authenticateUser method" should {
    // WARNING: Socrata-http requires that cookies have the form key=value, otherwise it will respond with
    // 'The target server failed to respond'

    "pass along requestid in core request" in {
      val userBody = j"""["bar"]"""
      val reqId = "42"
      val expectedRequest =
        request()
          .withMethod("GET")
          .withPath(s"/users.json")
          .withHeader(HeaderXSocrataHostKey, domain)
          .withHeader(HeaderXSocrataRequestIdKey, reqId)

      mockServer.when(
        expectedRequest
      ).respond(
        response()
          .withStatusCode(200)
          .withHeader("Content-Type", "application/json; charset=utf-8")
          .withBody(CompactJsonWriter.toString(userBody))
      )

      coreClient.authenticateUser(domain, AuthParams(cookie=Some(cookie)), Some(reqId))
      mockServer.verify(expectedRequest)
    }

    "pass back set-cookie from the core response" in {
      val userBody = j"""["bar"]"""
      val expectedSetCookie = "everything=awesome"

      mockServer.when(
        request()
          .withMethod("GET")
          .withPath(s"/users.json")
          .withHeader(HeaderXSocrataHostKey, domain)
      ).respond(
        response()
          .withStatusCode(200)
          .withHeader("Content-Type", "application/json; charset=utf-8")
          .withHeader(HeaderSetCookieKey, expectedSetCookie)
          .withBody(CompactJsonWriter.toString(userBody))
      )

      val (_, setCookies) = coreClient.authenticateUser(domain, AuthParams(cookie=Some(cookie)), None)
      setCookies should contain theSameElementsAs Seq(expectedSetCookie)
    }

    "return the user if core returns a 200 and an empowered user" in {
      setUpMockWithAuthorizedUser()
      val expectedUser = User("boo-bear", roleName = Some("headBear"),
        rights = Some(List("steal_honey", "scare_tourists")), flags = Some(List("admin")))

      val (actualUser, _) = coreClient.authenticateUser(domain, AuthParams(cookie=Some(cookie)), None)
      actualUser.get should be(expectedUser)
    }

    "return the user if core returns a 200 and powerless user" in {
      setUpMockWithUnauthorizeddUser()
      val expectedUser = User("lazy-bear", roleName = None, rights = None, flags = None)

      val (actualUser, _) = coreClient.authenticateUser(domain, AuthParams(cookie=Some(cookie)), None)
      actualUser.get should be(expectedUser)
    }


    "return None without calling core if passed an empty cookie" in {
      mockServer.when(
        request()
          .withMethod("GET")
          .withPath("/users.json")
          .withHeader(HeaderXSocrataHostKey, domain)
          .withHeader(HeaderCookieKey, "")
      ).respond(
        response()
          .withStatusCode(401)
          .withHeader("Content-Type", "application/json; charset=utf-8")
      )

      coreClient.authenticateUser(domain, AuthParams(cookie=Some("")), None)._1 should be(None)
    }

    "return None if core returns a 401" in {
      mockServer
        .when(
          request()
            .withMethod("GET")
            .withPath("/users.json")
        )
        .respond(
          response()
            .withStatusCode(401)
        )

      coreClient.authenticateUser(domain, AuthParams(cookie=Some(cookie)), None)._1 should be(None)
    }

    "return None if core returns a 403" in {
      mockServer
        .when(
          request()
            .withMethod("GET")
            .withPath("/users.json")
        )
        .respond(
          response()
            .withStatusCode(403)
        )

      coreClient.authenticateUser(domain, AuthParams(cookie=Some(cookie)), None)._1 should be(None)
    }

    "return None if core returns a 500" in {
      mockServer
        .when(
          request()
            .withMethod("GET")
            .withPath("/users.json")
        )
        .respond(
          response()
            .withStatusCode(500)
        )

      coreClient.authenticateUser(domain, AuthParams(cookie=Some(cookie)), None)._1 should be(None)
    }
  }

  "The fetchUserById method" should {
    "pass along requestid in core request" in {
      val fxf = "foo-bear"
      val userBody = j"""["bar"]"""
      val reqId = "42"
      val expectedRequest =
        request()
          .withMethod("GET")
          .withPath(s"/users/$fxf")
          .withHeader(HeaderXSocrataHostKey, domain)
          .withHeader(HeaderXSocrataRequestIdKey, reqId)

      mockServer.when(
        expectedRequest
      ).respond(
        response()
          .withStatusCode(200)
          .withHeader("Content-Type", "application/json; charset=utf-8")
          .withBody(CompactJsonWriter.toString(userBody))
      )

      coreClient.fetchUserById(domain, fxf, Some(reqId))
      mockServer.verify(expectedRequest)
    }

    "pass back set-cookie from the core response" in {
      val fxf = "foo-bear"
      val userBody = j"""["bar"]"""
      val expectedSetCookie = "everything=awesome"

      mockServer.when(
        request()
          .withMethod("GET")
          .withPath(s"/users/$fxf")
          .withHeader(HeaderXSocrataHostKey, domain)
      ).respond(
        response()
          .withStatusCode(200)
          .withHeader("Content-Type", "application/json; charset=utf-8")
          .withHeader(HeaderSetCookieKey, expectedSetCookie)
          .withBody(CompactJsonWriter.toString(userBody))
      )

      val (_, setCookies) = coreClient.fetchUserById(domain, fxf, None)
      setCookies should contain theSameElementsAs Seq(expectedSetCookie)
    }

    "return the user if core returns a 200" in {
      val fxf = "boo-bear"
      val userBody =
        j"""{
        "id" : "boo-bear",
        "screenName" : "boo bear",
        "numberOfFollowers" : 1000,
        "numberOfFriends" : 500,
        "roleName" : "headBear",
        "rights" : [ "steal_honey", "scare_tourists"],
        "flags" : [ "admin" ]
        }"""

      mockServer.when(
        request()
          .withMethod("GET")
          .withPath(s"/users/$fxf")
          .withHeader(HeaderXSocrataHostKey, domain)
      ).respond(
        response()
          .withStatusCode(200)
          .withHeader("Content-Type", "application/json; charset=utf-8")
          .withBody(CompactJsonWriter.toString(userBody))
      )

      val expectedUser = User("boo-bear", roleName = Some("headBear"),
        rights = Some(List("steal_honey", "scare_tourists")), flags = Some(List("admin")))

      val (actualUser, _) = coreClient.fetchUserById(domain, fxf, None)
      actualUser.get should be(expectedUser)
    }

    "return None if the core returns unexpected json" in {
      val fxf = "oops-bear"
      val userBody =
        j"""{
        "screenName" : "oops bear",
        "rights" : [ "hibernate"]
        }"""

      mockServer.when(
        request()
          .withMethod("GET")
          .withPath(s"/users/$fxf")
          .withHeader(HeaderXSocrataHostKey, domain)
      ).respond(
        response()
          .withStatusCode(200)
          .withHeader("Content-Type", "application/json; charset=utf-8")
          .withBody(CompactJsonWriter.toString(userBody))
      )
      coreClient.fetchUserById(domain, fxf, None)._1 should be(None)
    }

    "return None if core returns a 405" in {
      val fxf = "four-four"
      mockServer
        .when(
          request()
            .withMethod("GET")
            .withPath(s"/users/$fxf")
        )
        .respond(
          response()
            .withStatusCode(405)
        )

      coreClient.fetchUserById(domain, fxf, None)._1 should be(None)
    }
  }
}

// no mock-server set up for these tests
class CoreClientlessSpec extends WordSpec with ShouldMatchers {

  val coreTestPort = 8016
  val httpClient = new TestHttpClient()
  val coreClient = new TestCoreClient(httpClient, coreTestPort)
  val domain = "forest.com"
  val cookie = "c=cookie"

  "When core is not reachable, the authenticateUser method" should {
    "return None" in {
      coreClient.authenticateUser(domain, AuthParams(cookie=Some(cookie)), None)._1 should be(None)
    }
  }

  "When core is not reachable, the fetchUserById method" should {
    "return None" in {
      coreClient.fetchUserById(domain, "four-four", None)._1 should be(None)
    }
  }
}
