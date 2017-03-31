package com.socrata.cetera.services


import java.io.{ByteArrayInputStream, ByteArrayOutputStream}
import java.util.Collections
import javax.servlet.http.{HttpServletRequest, HttpServletResponse}
import scala.collection.JavaConverters._

import com.rojoma.json.v3.interpolation._
import com.rojoma.json.v3.io.CompactJsonWriter
import com.socrata.http.server.HttpRequest
import com.socrata.http.server.HttpRequest.AugmentedHttpServletRequest
import org.elasticsearch.action.search.{SearchRequestBuilder, SearchAction}
import org.mockserver.integration.ClientAndServer._
import org.mockserver.model.HttpRequest._
import org.mockserver.model.HttpResponse._
import org.scalamock.scalatest.proxy.MockFactory
import org.scalatest.{BeforeAndAfterAll, FunSuiteLike, Matchers}
import org.springframework.mock.web.{DelegatingServletInputStream, DelegatingServletOutputStream}

import com.socrata.cetera.auth.User
import com.socrata.cetera.handlers.{PagingParamSet, ScoringParamSet, SearchParamSet}
import com.socrata.cetera.metrics.BalboaClient
import com.socrata.cetera.search.{BaseDocumentClient, BaseDomainClient}
import com.socrata.cetera.types.DomainSet
import com.socrata.cetera.{response => _, _}

class SearchServiceJettySpec extends FunSuiteLike with Matchers with MockFactory with BeforeAndAfterAll {
  val testSuiteName = getClass.getSimpleName.toLowerCase
  val client = new TestESClient(testSuiteName)
  val coreTestPort = 8037
  val mockCoreServer = startClientAndServer(coreTestPort)
  val httpClient = new TestHttpClient()
  val coreClient = new TestCoreClient(httpClient, coreTestPort)
  val mockDomainClient = mock[BaseDomainClient]
  val mockDocumentClient = mock[BaseDocumentClient]
  val balboaClient = new BalboaClient("/tmp/metrics")
  val service = new SearchService(mockDocumentClient, mockDomainClient, balboaClient, coreClient)

  override protected def afterAll(): Unit = {
    client.close()
    mockCoreServer.stop(true)
    httpClient.close()
  }

  test("pass on any set-cookie headers in response") {
    val hostCname = "test.com"
    val userBody = j"""{"id" : "some-user"}"""
    val authedUser = User(userBody)
    val expectedSetCookie = Seq("life=42", "universe=42", "everything=42")

    mockCoreServer.when(
      request()
        .withMethod("GET")
        .withPath("/users.json")
        .withHeader(HeaderXSocrataHostKey, hostCname)
    ).respond(
      response()
        .withStatusCode(200)
        .withHeader("Content-Type", "application/json; charset=utf-8")
        .withHeader("Set-Cookie", expectedSetCookie: _*)
        .withBody(CompactJsonWriter.toString(userBody))
    )

    mockDomainClient.expects('findSearchableDomains)(None, Some(hostCname), None, true, authedUser, Some("1"))
      .returns((DomainSet(), 123L))

    mockDocumentClient.expects('buildSearchRequest)(
      DomainSet(), SearchParamSet(), ScoringParamSet(), PagingParamSet(), authedUser)
      .returns(new SearchRequestBuilder(client.client, SearchAction.INSTANCE))

    val servReq = mock[HttpServletRequest]
    servReq.expects('getMethod)().anyNumberOfTimes.returns("GET")
    servReq.expects('getRequestURI)().anyNumberOfTimes.returns("/test")
    servReq.expects('getHeaderNames)().anyNumberOfTimes.returns(Collections.emptyEnumeration[String]())
    servReq.expects('getInputStream)().anyNumberOfTimes.returns(
      new DelegatingServletInputStream(new ByteArrayInputStream("".getBytes)))
    servReq.expects('getHeader)(HeaderCookieKey).returns("c=cookie")
    servReq.expects('getHeader)(HeaderXSocrataHostKey).anyNumberOfTimes.returns(hostCname)
    servReq.expects('getHeader)(HeaderXSocrataRequestIdKey).anyNumberOfTimes.returns("1")
    servReq.expects('getHeader)(HeaderAuthorizationKey).anyNumberOfTimes.returns("")
    servReq.expects('getQueryString)().anyNumberOfTimes.returns("")
    servReq.expects('getRemoteHost)().anyNumberOfTimes.returns("remotehost")

    val augReq = new AugmentedHttpServletRequest(servReq)
    val httpReq = mock[HttpRequest]
    httpReq.expects('servletRequest)().anyNumberOfTimes.returning(augReq)

    val outStream = new ByteArrayOutputStream()
    val httpResponse = mock[HttpServletResponse]
    val status: Integer = 200
    httpResponse.expects('setStatus)(status)
    httpResponse.expects('setHeader)("Access-Control-Allow-Origin", "*")
    httpResponse.expects('setContentType)("application/json; charset=UTF-8")
    expectedSetCookie.foreach { s => httpResponse.expects('addHeader)("Set-Cookie", s)}
    httpResponse.expects('getHeaderNames)().anyNumberOfTimes.returns(Seq("Set-Cookie").asJava)
    httpResponse.expects('getHeaders)("Set-Cookie").anyNumberOfTimes.returns(expectedSetCookie.asJava)
    httpResponse.expects('getOutputStream)().returns(new DelegatingServletOutputStream(outStream))

    service.search(httpReq)(httpResponse)
  }
}
