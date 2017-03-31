package com.socrata.cetera.services

import java.io.ByteArrayInputStream
import java.nio.charset.{Charset, CodingErrorAction}
import java.util.Collections
import javax.servlet.http.HttpServletRequest
import org.elasticsearch.index.Index
import org.elasticsearch.search.profile.{ ProfileShardResult, SearchProfileShardResults }
import scala.collection.JavaConverters._

import com.rojoma.json.v3.ast.{JString, JValue}
import com.rojoma.json.v3.interpolation._
import com.rojoma.simplearm.v2._
import com.socrata.http.server.HttpRequest
import com.socrata.http.server.HttpRequest.AugmentedHttpServletRequest
import org.apache.http.HttpStatus.SC_INTERNAL_SERVER_ERROR
import org.elasticsearch.action.search._
import org.elasticsearch.common.bytes.BytesArray
import org.elasticsearch.common.text.Text
import org.elasticsearch.search.aggregations.{InternalAggregation, InternalAggregations}
import org.elasticsearch.search.internal._
import org.elasticsearch.search.suggest.Suggest
import org.elasticsearch.search.{SearchHitField, SearchShardTarget}
import org.scalamock.scalatest.proxy.MockFactory
import org.scalatest.{BeforeAndAfterAll, BeforeAndAfterEach, FunSuiteLike, Matchers}
import org.springframework.mock.web.{DelegatingServletInputStream, MockHttpServletResponse}

import com.socrata.cetera._
import com.socrata.cetera.auth.AuthParams
import com.socrata.cetera.errors.UnauthorizedError
import com.socrata.cetera.handlers.FormatParamSet
import com.socrata.cetera.response.{Classification, Format, SearchResult, SearchResults}
import com.socrata.cetera.types._

class SearchServiceSpec extends FunSuiteLike
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

  val emptySearchHitMap = Map[String, SearchHitField]().asJava
  val domainSet = DomainSet(domains = (0 to 2).map(domains(_)).toSet)

  val searchResponse = {
    val shardTarget = new SearchShardTarget("1", new Index("catalog", "foo"), 1)
    val score = 0.12345f

    val resource = "\"resource\":{\"name\": \"Just A Test\", \"I'm\":\"OK\",\"you're\":\"so-so\"}"

    val datasetSocrataId =
      "\"socrata_id\":{\"domain_id\":[0],\"dataset_id\":\"four-four\"}"
    val pageSocrataId =
      "\"socrata_id\":{\"domain_id\":[1,2],\"dataset_id\":\"four-four\",\"page_id\":\"fore-fore\"}"

    val datasetDatatype = "\"datatype\":\"dataset\""
    val datasetViewtype = "\"viewtype\":\"\""
    val pageDatatype = "\"datatype\":\"datalens\""
    val pageViewtype = "\"viewtype\":\"\""

    val datasetSource = new BytesArray("{" + List(resource, datasetDatatype, datasetViewtype, datasetSocrataId).mkString(",") + "}")
    val pageSource = new BytesArray("{" + List(resource, pageDatatype, pageViewtype, pageSocrataId).mkString(",") + "}")

    val datasetHit = new InternalSearchHit(1, "46_3yu6-fka7", new Text("dataset"), emptySearchHitMap)
    datasetHit.shardTarget(shardTarget)
    datasetHit.sourceRef(datasetSource)
    datasetHit.score(score)

    val pageHit = new InternalSearchHit(1, "64_6uy3-7akf", new Text("page"), emptySearchHitMap)
    pageHit.shardTarget(shardTarget)
    pageHit.sourceRef(pageSource)
    pageHit.score(score)

    val hits = Array[InternalSearchHit](datasetHit, pageHit)
    val internalSearchHits = new InternalSearchHits(hits, 3037, 1.0f)
    val internalSearchResponse = new InternalSearchResponse(
      internalSearchHits,
      new InternalAggregations(List[InternalAggregation]().asJava),
      new Suggest(),
      new SearchProfileShardResults(Map.empty[String, ProfileShardResult].asJava),
      false,
      false)

    new SearchResponse(internalSearchResponse, "", 15, 15, 4, Array[ShardSearchFailure]())
  }

  val badSearchResponse = {
    val shardTarget = new SearchShardTarget("1", new Index("catalog", "foo"), 1)
    val score = 0.54321f

    val badResource = "\"badResource\":{\"name\": \"Just A Test\", \"I'm\":\"NOT OK\",\"you'll\":\"never know\"}"
    val goodResource = "\"resource\":{\"name\": \"Just A Test\", \"I'm\":\"OK\",\"you're\":\"so-so\"}"

    val datasetSocrataId = "\"socrata_id\":{\"domain_id\":[0],\"dataset_id\":\"four-four\"}"
    val badDatasetSocrataId = "\"socrata_id\":{\"domain_id\":\"i am a string\",\"dataset_id\":\"four-four\"}"
    val pageSocrataId = "\"socrata_id\":{\"domain_id\":[1,2],\"dataset_id\":\"four-four\",\"page_id\":\"fore-fore\"}"

    val datasetDatatype = "\"datatype\":\"dataset\""
    val datasetViewtype = "\"viewtype\":\"\""
    val pageDatatype = "\"datatype\":\"datalens\""
    val pageViewtype = "\"viewtype\":\"\""

    val badResourceDatasetSource = new BytesArray("{" +
      List(badResource, datasetDatatype, datasetViewtype, datasetSocrataId).mkString(",") +
      "}")

    val badSocrataIdDatasetSource = new BytesArray("{" +
      List(goodResource, datasetDatatype, datasetViewtype, badDatasetSocrataId).mkString(",")
      + "}")

    val pageSource = new BytesArray("{" +
      List(goodResource, pageDatatype, pageViewtype, pageSocrataId).mkString(",")
      + "}")

    // A result missing the resource field should get filtered (a bogus doc is often missing expected fields)
    val badResourceDatasetHit = new InternalSearchHit(1, "46_3yu6-fka7", new Text("dataset"), emptySearchHitMap)
    badResourceDatasetHit.shardTarget(shardTarget)
    badResourceDatasetHit.sourceRef(badResourceDatasetSource)
    badResourceDatasetHit.score(score)

    // A result with corrupted (unparseable) field should also get skipped (instead of raising)
    val badSocrataIdDatasetHit = new InternalSearchHit(1, "46_3yu6-fka7", new Text("dataset"), emptySearchHitMap)
    badSocrataIdDatasetHit.shardTarget(shardTarget)
    badSocrataIdDatasetHit.sourceRef(badSocrataIdDatasetSource)
    badSocrataIdDatasetHit.score(score)

    val pageHit = new InternalSearchHit(1, "64_6uy3-7akf", new Text("page"), emptySearchHitMap)
    pageHit.shardTarget(shardTarget)
    pageHit.sourceRef(pageSource)
    pageHit.score(score)

    val hits = Array[InternalSearchHit](badResourceDatasetHit, badSocrataIdDatasetHit, pageHit)
    val internalSearchHits = new InternalSearchHits(hits, 3037, 1.0f)
    val internalSearchResponse = new InternalSearchResponse(
      internalSearchHits,
      new InternalAggregations(List[InternalAggregation]().asJava),
      new Suggest(),
      new SearchProfileShardResults(Map.empty[String, ProfileShardResult].asJava),
      false,
      false)

    new SearchResponse(internalSearchResponse, "", 15, 15, 4, Array[ShardSearchFailure]())
  }

  test("extract and format resources from SearchResponse") {
    val domain = Domain(1, "tempuri.org", Some("Title"), Some("Temp Org"), isCustomerDomain = true, moderationEnabled = false, routingApprovalEnabled = false, lockedDown = false, apiLockedDown = false)
    val resource = j"""{ "name" : "Just A Test", "I'm" : "OK", "you're" : "so-so" }"""

    val searchResults = Format.formatDocumentResponse(searchResponse, None, domainSet, FormatParamSet())

    searchResults.resultSetSize should be (searchResponse.getHits.getTotalHits)
    searchResults.timings should be (None) // not yet added

    val results = searchResults.results
    results should be ('nonEmpty)
    results.size should be (2)

    val datasetResponse = results(0)
    datasetResponse.resource should be (j"""${resource}""")
    datasetResponse.classification should be (Classification(Seq.empty[JValue], Seq.empty[JValue], None, None, None, None))

    datasetResponse.metadata.domain should be ("petercetera.net")

    val pageResponse = results(1)
    pageResponse.resource should be (j"""${resource}""")
    pageResponse.classification should be (Classification(Seq.empty[JValue], Seq.empty[JValue], None, None, None, None))

    pageResponse.metadata.domain should be ("blue.org")
  }

  test("SearchResponse does not throw on bad documents it just ignores them") {
    val domain = Domain(1, "tempuri.org", Some("Title"), Some("Temp Org"), isCustomerDomain = true, moderationEnabled = false, routingApprovalEnabled = false, lockedDown = false, apiLockedDown = false)

    val expectedResource = j"""{ "name" : "Just A Test", "I'm" : "OK", "you're" : "so-so" }"""
    val searchResults = Format.formatDocumentResponse(badSearchResponse, None, domainSet, FormatParamSet())

    val results = searchResults.results
    results.size should be (1)

    val pageResponse = results(0)
    pageResponse.resource should be (j"""${expectedResource}""")
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
      val fxf = hit.resource.dyn.id.!.asInstanceOf[JString].string
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
    val host = domains(3).domainCname
    val authedUserBody =
      j"""{
        "id" : "lil-john",
        "roleName" : "publisher",
        "rights" : [ "walk_though_forest", "laugh_back_and_forth", "reminisce" ]
        }"""
    val expectedVis = Map(
      "fxf-3" -> false,
      "fxf-7" -> false,
      "fxf-11" -> true,
      "fxf-12" -> true,
      "zeta-0002" -> true,
      "zeta-0009" -> false,
      "zeta-0013" -> false,
      "zeta-0014" -> false
    )

    prepareAuthenticatedUser(cookie, host, authedUserBody)
    val params = Map("domains" -> host, "search_context" -> host, "show_visibility" -> "true").mapValues(Seq(_))
    val res = service.doSearch(params, AuthParams(cookie = Some(cookie)), Some(host), None)._2
    fxfsVisibility(res) should contain theSameElementsAs expectedVis
  }

  test("federated search results also know their own visibility") {
    val host = domains(0).domainCname
    val authedUserBody =
      j"""{
        "id" : "cook-mons",
        "roleName" : "viewer",
        "rights" : [ "eat_cookies", "spell_words_starting_with_c" ]
        }"""
    val expectedVis = Map(
      "fxf-0" -> true,
      "fxf-4" -> false,
      "fxf-8" -> true,
      "zeta-0001" -> true,
      "zeta-0004" -> false,
      "zeta-0006" -> false,
      "zeta-0007" -> true,
      "zeta-0011" -> false,
      "zeta-0012" -> true
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

    val host = domain.domainCname
    val params = Map("only" -> "links", "domains" -> host, "search_context" -> host).mapValues(Seq(_))
    val res = service.doSearch(params, AuthParams(), None, None)
    val actualFxfs = fxfs(res._2)

    actualFxfs should contain theSameElementsAs(expectedFxfs)
  }

  test("searching for only=federated_hrefs should find only federated_hrefs") {
    val domain = domains(3)
    val domain3Docs = docs.filter(d => d.socrataId.domainId == 3)
    val expectedFxfs = fxfs(domain3Docs.filter(d => d.datatype == "federated_href"))

    val host = domain.domainCname
    val params = Map("only" -> "federated_hrefs", "domains" -> host, "search_context" -> host).mapValues(Seq(_))
    val res = service.doSearch(params, AuthParams(), None, None)
    val actualFxfs = fxfs(res._2)

    actualFxfs should contain theSameElementsAs(expectedFxfs)
  }

  ignore("es client - min should match") {}
  ignore("es client - slop") {}
  ignore("es client - function score") {}
  ignore("es client - advanced query") {}
  ignore("es client - score boosts") {}
  ignore("es client - domain metadata filter") {}
  ignore("es client - script score functions") {}
  ignore("es client - query with no filters, maybe?") {}
  ignore("es client - sort field asc/desc") {}
  ignore("domain cname unexpected json value") {}
  ignore("query parameter parser - errors") {}
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
