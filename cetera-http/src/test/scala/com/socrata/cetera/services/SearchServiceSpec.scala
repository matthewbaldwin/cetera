package com.socrata.cetera.services

import java.io.ByteArrayInputStream
import java.nio.charset.{Charset, CodingErrorAction}
import java.util.Collections
import javax.servlet.http.HttpServletRequest

import com.rojoma.json.v3.interpolation._
import com.rojoma.simplearm.v2._
import com.socrata.http.server.HttpRequest
import com.socrata.http.server.HttpRequest.AugmentedHttpServletRequest
import org.apache.commons.io.FileUtils
import org.apache.http.HttpStatus.SC_INTERNAL_SERVER_ERROR
import org.scalamock.scalatest.proxy.MockFactory
import org.scalatest.{BeforeAndAfterAll, BeforeAndAfterEach, FunSuiteLike, Matchers}
import org.springframework.mock.web.{DelegatingServletInputStream, MockHttpServletResponse}

import com.socrata.cetera._
import com.socrata.cetera.auth.AuthParams
import com.socrata.cetera.errors.UnauthorizedError
import com.socrata.cetera.response.{SearchResult, SearchResults}
import com.socrata.cetera.types._

class SearchServiceSpec extends FunSuiteLike
  with Matchers
  with TestESData
  with BeforeAndAfterAll
  with BeforeAndAfterEach {

  override protected def beforeAll(): Unit = {
    // Remove the contents of /tmp/metrics
    FileUtils.cleanDirectory(balboaDir)
    bootstrapData()
  }

  override def beforeEach(): Unit = mockServer.reset()

  override protected def afterAll(): Unit = {
    removeBootstrapData()
    client.close()
    httpClient.close()
  }

  def wasSearchQueryLogged(filename: String, q: String): Boolean = {
    val decoder = Charset.forName("UTF-8").newDecoder()
    decoder.onMalformedInput(CodingErrorAction.IGNORE)
    for {
      s <- managed(scala.io.Source.fromFile(filename)(decoder))
    } {
      val entries = s.getLines.toList.head
      entries.contains(s"datasets-search-$q")
    }
  }

  private def fxfsVisibility(searchResults: SearchResults[SearchResult]): Map[String, Boolean] =
    searchResults.results.map { hit =>
      val fxf = hit.resource.id
      val visibility = hit.metadata.visibleToAnonymous.getOrElse(fail())
      fxf -> visibility
    }.toMap

  test("if a domain is given in the searchContext and a simple query is given, the query should be logged") {
    val query = "log this query"
    val params = Map(
      "domains" -> "opendata-demo.socrata.com,petercetera.net",
      "search_context" -> "opendata-demo.socrata.com",
      "q" -> query
    ).mapValues(Seq(_))
    service.doSearch(params, AuthParams(), None, None)._2.results
    val metricsFile = balboaDir.listFiles()(0)
    wasSearchQueryLogged(metricsFile.getAbsolutePath, query) should be(true)
  }

  test("if a domain is given in the searchContext and an advanced query is given, the query should be logged") {
    val query = "(log this query OR don't) AND (check up on it OR don't)"
    val params = Map(
      "domains" -> "opendata-demo.socrata.com,petercetera.net",
      "search_context" -> "opendata-demo.socrata.com",
      "q_internal" -> query
    ).mapValues(Seq(_))
    service.doSearch(params, AuthParams(), None, None)._2.results
    val metricsFile = balboaDir.listFiles()(0)
    wasSearchQueryLogged(metricsFile.getAbsolutePath, query) should be(true)
  }

  test("if there is no searchContext, no query should be logged") {
    val query = "don't log this query"
    val params = Map(
      "domains" -> "opendata-demo.socrata.com,petercetera.net",
      "q" -> query
    ).mapValues(Seq(_))
    service.doSearch(params, AuthParams(), None, None)._2.results
    val metricsFile = balboaDir.listFiles()(0)
    wasSearchQueryLogged(metricsFile.getAbsolutePath, query) should be(false)
  }

  test("when requested, include post-calculated anonymous visibility field") {
    val host = domains(3).cname
    val authedUserBody =
      j"""{
        "id" : "lil-john",
        "roleName" : "publisher",
        "rights" : [ "view_others_datasets", "view_story" ]
        }"""
    val expectedVis = Map(
      "d3-v0" -> false,
      "d3-v1" -> false,
      "d3-v2" -> true,
      "d3-v3" -> true,
      "d3-v4" -> true,
      "d3-v5" -> false,
      "d3-v6" -> false,
      "d3-v7" -> false,
      "d3-v8" -> false,
      "d3-v9" -> false
    )

    prepareAuthenticatedUser(cookie, host, authedUserBody)
    val params = Map("domains" -> host, "search_context" -> host, "show_visibility" -> "true").mapValues(Seq(_))
    val res = service.doSearch(params, AuthParams(cookie = Some(cookie)), Some(host), None)._2
    fxfsVisibility(res) should contain theSameElementsAs expectedVis
  }

  test("federated search results also know their own visibility") {
    val host = domains(0).cname
    val authedUserBody =
      j"""{
        "id" : "cook-mons",
        "roleName" : "viewer",
        "rights" : [ "view_others_datasets", "view_story" ]
        }"""
    val expectedVis = Map(
      "d0-v0" -> true,
      "d0-v1" -> false,
      "d0-v2" -> true,
      "d0-v3" -> true,
      "d0-v4" -> true,
      "d0-v5" -> false,
      "d0-v6" -> false,
      "d0-v7" -> true,
      "d0-v8" -> false,
      "d0-v9" -> true
    )

    prepareAuthenticatedUser(cookie, host, authedUserBody)
    val params = Map("domains" -> host, "search_context" -> host, "show_visibility" -> "true").mapValues(Seq(_))
    val res = service.doSearch(params, AuthParams(cookie = Some(cookie)), Some(host), None)._2
    fxfsVisibility(res) should contain theSameElementsAs expectedVis
  }

  test("searching for assets shared to anyone except logged in user throws an unauthorizedError") {
    val host = "petercetera.net"
    val authedUserBody =
      j"""{
        "id" : "No One",
        "roleName" : "nothing",
        "rights" : [ "nothing" ]
        }"""

    prepareAuthenticatedUser(cookie, host, authedUserBody)
    intercept[UnauthorizedError] {
      val params =  Map("shared_to" -> Seq("Different Person"))
      service.doSearch(params, AuthParams(cookie=Some(cookie)), Some(host), None)
    }
  }

  test("searching for apis should not throw an error, but simply return no results") {
    val host = "petercetera.net"
    val params = Map("only" -> "apis").mapValues(Seq(_))
    val res = service.doSearch(params, AuthParams(), None, None)._2.results
    res should be('empty)
  }

  test("searching for only=links should find both hrefs and federated_hrefs") {
    val domain = domains(3)
    val domain3Docs = docs.filter(d => d.socrataId.domainId == 3)
    val expectedFxfs = fxfs(domain3Docs.filter(d => d.datatype == "href" || d.datatype == "federated_href"))

    val host = domain.cname
    val params = Map("only" -> "links", "domains" -> host, "search_context" -> host).mapValues(Seq(_))
    val res = service.doSearch(params, AuthParams(), None, None)
    val actualFxfs = fxfs(res._2)

    actualFxfs should contain theSameElementsAs(expectedFxfs)
  }

  test("searching for submitter_id should find results with that submitter") {
    val domain = domains(0)
    val domain0Docs = anonymouslyViewableDocs.filter(d => d.socrataId.domainId == 0)
    val expectedFxfs = fxfs(domain0Docs.filter(d => d.approvals.isDefined && d.approvals.get.forall(_.submitterId == "robin-hood")))

    val host = domain.cname
    val params = Map("submitter_id" -> "robin-hood", "domains" -> host, "search_context" -> host).mapValues(Seq(_))
    val res = service.doSearch(params, AuthParams(), None, None)
    val actualFxfs = fxfs(res._2)

    actualFxfs should contain theSameElementsAs(expectedFxfs)
  }

  test("searching for reviewer_id should find results with that reviewer") {
    val domain = domains(0)
    val domain0Docs = anonymouslyViewableDocs.filter(d => d.socrataId.domainId == 0)
    val expectedFxfs = fxfs(domain0Docs.filter(d => d.approvals.isDefined && d.approvals.get.forall(_.reviewerId.getOrElse("") == "honorable.sheriff")))

    val host = domain.cname
    val params = Map("reviewer_id" -> "honorable.sheriff", "domains" -> host, "search_context" -> host).mapValues(Seq(_))
    val res = service.doSearch(params, AuthParams(), None, None)
    val actualFxfs = fxfs(res._2)

    actualFxfs should contain theSameElementsAs(expectedFxfs)
  }

  test("searching for reviewed_automatically=true should find results that were automatically reviewed") {
    val domain = domains(0)
    val domain0Docs = anonymouslyViewableDocs.filter(d => d.socrataId.domainId == 0)
    val expectedFxfs = fxfs(domain0Docs.filter(d => d.approvals.isDefined && d.approvals.get.forall(_.reviewedAutomatically.getOrElse(false))))

    val host = domain.cname
    val params = Map("reviewed_automatically" -> "true", "domains" -> host, "search_context" -> host).mapValues(Seq(_))
    val res = service.doSearch(params, AuthParams(), None, None)
    val actualFxfs = fxfs(res._2)

    actualFxfs should contain theSameElementsAs(expectedFxfs)
  }

  test("searching for reviewed_automatically=false should find results that were reviewed by humans") {
    val domain = domains(0)
    val domain0Docs = anonymouslyViewableDocs.filter(d => d.socrataId.domainId == 0)
    val expectedFxfs = fxfs(domain0Docs.filter(d => d.approvals.isDefined && d.approvals.get.forall(!_.reviewedAutomatically.getOrElse(true))))

    val host = domain.cname
    val params = Map("reviewed_automatically" -> "false", "domains" -> host, "search_context" -> host).mapValues(Seq(_))
    val res = service.doSearch(params, AuthParams(), None, None)
    val actualFxfs = fxfs(res._2)

    actualFxfs should contain theSameElementsAs(expectedFxfs)
  }

  test("searching for only=federated_hrefs should find only federated_hrefs") {
    val domain = domains(3)
    val domain3Docs = docs.filter(d => d.socrataId.domainId == 3)
    val expectedFxfs = fxfs(domain3Docs.filter(d => d.datatype == "federated_href"))

    val host = domain.cname
    val params = Map("only" -> "federated_hrefs", "domains" -> host, "search_context" -> host).mapValues(Seq(_))
    val res = service.doSearch(params, AuthParams(), None, None)
    val actualFxfs = fxfs(res._2)

    actualFxfs should contain theSameElementsAs(expectedFxfs)
  }

  test("a query that matches two otherwise identical documents, will prefer the more recently updated") {
    val params = Map(
      "domains" -> "robert.demo.socrata.com",
      "show_score" -> "true",
      "q" -> "latest and greatest",
      "age_decay" -> "gauss,182d,0.5,14d,2017-04-04"
    ).mapValues(Seq(_))

    val results = service.doSearch(params, AuthParams(), None, None)._2.results
    results.map(result =>
      result.resource.id
    ) should contain inOrderOnly ("d9-v0", "d9-v1")
  }

  test("if scroll_id is specified, then results will be sorted by dataset ID") {
    val params = Map("scroll_id" -> Seq(""), "domains" -> Seq(domains.map(_.cname).mkString(",")))

    val results = service.doSearch(params, AuthParams(), None, None)._2.results

    results.map(result =>
      result.resource.id
    ) should contain theSameElementsInOrderAs anonymouslyViewableDocIds.sorted
  }

  test("the expected results are returned when scroll_id and limit are specified") {
    val params = Map("scroll_id" -> Seq("d0-v2"), "limit" -> Seq("5"), "domains" -> Seq(domains.map(_.cname).mkString(",")))
    val anonymouslyViewableDocIdsSorted = anonymouslyViewableDocIds.sorted
    val dropIndex = anonymouslyViewableDocIdsSorted.indexOf("d0-v2") + 1

    val results = service.doSearch(params, AuthParams(), None, None)._2.results

    results.map(result =>
      result.resource.id
    ) should contain theSameElementsInOrderAs anonymouslyViewableDocIdsSorted.drop(dropIndex).take(5)
  }

  test("the owner field is included in the results") {
    val params = Map("domains" -> Seq("robert.demo.socrata.com"), "ids" -> Seq("d9-v0"))
    val results = service.doSearch(params, AuthParams(), None, None)._2.results
    results.toList(0).owner should be(UserInfo("honorable.sheriff", Some("Honorable Sheriff of Nottingham")))
  }

  test("sorting on the owner returns results in the correct order") {
    val params = Map("domains" -> Seq("robert.demo.socrata.com"), "ids" -> Seq("d9-v0", "d9-v4"), "order" -> Seq("owner ASC"))

    val results = service.doSearch(params, AuthParams(), None, None)._2.results

    results.toList.map(_.owner.id) should be(List("honorable.sheriff", "lil-john"))

    val paramsDesc = params + ("order" -> Seq("owner DESC"))
    val resultsDesc = service.doSearch(paramsDesc, AuthParams(), None, None)._2.results

    resultsDesc.toList.map(_.owner.id) should be(List("lil-john", "honorable.sheriff"))
  }

  test("sorting on domain category returns results in the correct order") {
    val params = Map("domains" -> Seq("robert.demo.socrata.com"), "ids" -> Seq("d9-v0", "d9-v4"), "order" -> Seq("domain_category ASC"))

    val results = service.doSearch(params, AuthParams(), None, None)._2.results

    val t = results.toList.flatMap(
      _.classification.domainCategory
    ) should be(List("Beta", "Science"))

    val paramsDesc = params + ("order" -> Seq("domain_category DESC"))
    val resultsDesc = service.doSearch(paramsDesc, AuthParams(), None, None)._2.results

    resultsDesc.toList.flatMap(
      _.classification.domainCategory
    ) should be(List("Science", "Beta"))
  }

  test("sorting on datatype returns results in the correct order") {
    val params = Map("domains" -> Seq("robert.demo.socrata.com"), "order" -> Seq("datatype ASC"))

    val results = service.doSearch(params, AuthParams(), None, None)._2.results

    results.toList.map(
      _.resource.datatype
    ) should be(List("chart", "chart", "dataset", "dataset", "dataset"))

    val paramsDesc = params + ("order" -> Seq("datatype DESC"))
    val resultsDesc = service.doSearch(paramsDesc, AuthParams(), None, None)._2.results

    resultsDesc.toList.map(
      _.resource.datatype
    ) should be(List("dataset", "dataset", "dataset", "chart", "chart"))
  }

  test("assets with missing values for a sort key show up last when sorting ascending") {
    val params = Map("order" -> Seq("domain_category ASC"), "limit" -> Seq("100"))
    val results = service.doSearch(params, AuthParams(), None, None)._2.results

    val categories = results.toList.map(
      _.classification.domainCategory
    )

    categories.head should be(Some("Alpha"))

    def lastTwo = categories.drop(categories.length - 2)

    lastTwo should be(List(None, None))
  }

  test("assets with missing values for a sort key show up last when sorting descending") {
    val params = Map("order" -> Seq("domain_category DESC"), "limit" -> Seq("100"))
    val results = service.doSearch(params, AuthParams(), None, None)._2.results

    val categories = results.toList.map(
      _.classification.domainCategory
    )

    categories.head should be(Some("Science"))

    def lastTwo = categories.drop(categories.length - 2)

    lastTwo should be(List(None, None))
  }
}

class SearchServiceSpecWithBrokenES extends FunSuiteLike with Matchers with MockFactory with TestESData {
  //  ES is broken within this class because it's not Bootstrapped

  test("non fatal exceptions throw friendly error string") {
    val expectedResults = """{"error":"We're sorry. Something went wrong."}"""

    val servReq = mock[HttpServletRequest]
    servReq.expects('getMethod)().anyNumberOfTimes.returns("GET")
    servReq.expects('getRequestURI)().anyNumberOfTimes.returns("/test")
    servReq.expects('getHeaderNames)().anyNumberOfTimes.returns(Collections.emptyEnumeration[String]())
    servReq.expects('getInputStream)().anyNumberOfTimes.returns(new DelegatingServletInputStream(new ByteArrayInputStream("".getBytes)))
    servReq.expects('getHeader)(HeaderAuthorizationKey).anyNumberOfTimes.returns("Basic ricky:awesome")
    servReq.expects('getHeader)(HeaderAuthorizationKey).anyNumberOfTimes.returns("OAuth 123456789")
    servReq.expects('getHeader)(HeaderCookieKey).returns("ricky=awesome")
    servReq.expects('getHeader)(HeaderXSocrataHostKey).anyNumberOfTimes.returns(null)
    servReq.expects('getHeader)(HeaderXSocrataRequestIdKey).anyNumberOfTimes.returns("1")
    servReq.expects('getQueryString)().anyNumberOfTimes.returns("only=datasets")

    val augReq = new AugmentedHttpServletRequest(servReq)

    val httpReq = mock[HttpRequest]
    httpReq.expects('servletRequest)().anyNumberOfTimes.returning(augReq)

    val response = new MockHttpServletResponse()

    service.search(httpReq)(response)
    response.getStatus should be (SC_INTERNAL_SERVER_ERROR)
    response.getHeader("Access-Control-Allow-Origin") should be ("*")
    response.getContentAsString should be (expectedResults)
  }
}
