package com.socrata.cetera.services

import javax.servlet.http.HttpServletRequest

import com.rojoma.json.v3.ast.{JNumber, JString}
import com.rojoma.json.v3.codec.DecodeError
import com.rojoma.json.v3.interpolation._
import com.socrata.http.server.HttpRequest
import com.socrata.http.server.HttpRequest.AugmentedHttpServletRequest
import org.apache.http.HttpStatus.SC_INTERNAL_SERVER_ERROR
import org.scalamock.scalatest.proxy.MockFactory
import org.scalatest.{BeforeAndAfterAll, FunSuiteLike, Matchers}
import org.springframework.mock.web.MockHttpServletResponse

import com.socrata.cetera._
import com.socrata.cetera.auth.AuthParams
import com.socrata.cetera.errors.DomainNotFoundError
import com.socrata.cetera.handlers.Params
import com.socrata.cetera.response.SearchResults
import com.socrata.cetera.search._
import com.socrata.cetera.types._

class CountServiceSpec extends FunSuiteLike with Matchers with BeforeAndAfterAll {
  val testSuiteName = getClass.getSimpleName.toLowerCase
  val esClient = new TestESClient(testSuiteName)
  val httpClient = new TestHttpClient()
  val coreClient = new TestCoreClient(httpClient, 8031)
  val domainClient = new DomainClient(esClient, coreClient, testSuiteName)
  val documentClient = new DocumentClient(esClient, domainClient, testSuiteName, None, None, Set.empty, None)
  val service = new CountService(documentClient, domainClient, coreClient)

  override protected def afterAll(): Unit = {
    esClient.close() // Important!!
    httpClient.close()
  }

  val esResponse = j"""{
    "took" : 1,
    "timed_out" : false,
    "_shards" : {
      "total" : 10,
      "successful" : 10,
      "failed" : 0
    },
    "hits" : {
      "total" : 1862,
      "max_score" : 0.0,
      "hits" : [ ]
    },
    "aggregations" : {
      "domains" : {
        "doc_count_error_upper_bound" : 0,
        "sum_other_doc_count" : 0,
        "buckets" : [ {
          "key" : "onethousand.example.com",
          "doc_count" : 1000
        }, {
          "key" : "two-thirty-four.example.com",
          "doc_count" : 234
        }, {
          "key" : "seven-ate-nine.com",
          "doc_count" : 78
        }, {
          "key" : "poor-bono.example.com",
          "doc_count" : 1
        } ]
      }
    }
  }"""

  test("extract") {
    val expected = Stream(
      j"""{ "key" : "onethousand.example.com", "doc_count" : 1000 }""",
      j"""{ "key" : "two-thirty-four.example.com", "doc_count" : 234 }""",
      j"""{ "key" : "seven-ate-nine.com", "doc_count" : 78 }""",
      j"""{ "key" : "poor-bono.example.com", "doc_count" : 1 }"""
    )

    service.extract(esResponse) match {
      case Right(actual) =>
        (actual, expected).zipped.foreach{ (a, e) => a should be(e) }

      case Left(e) =>
        fail(e.toString)
    }
  }

  test("format") {
    val expected = SearchResults[Count](
      List(
        Count(JString("onethousand.example.com"), JNumber(1000)),
        Count(JString("two-thirty-four.example.com"),  JNumber(234)),
        Count(JString("seven-ate-nine.com"),  JNumber(78)),
        Count(JString("poor-bono.example.com"),  JNumber(1))
      ), 4
    )

    service.extract(esResponse) match {
      case Right(extracted) =>
        val formatted = service.format(extracted)
        val results = formatted.results
        results.zip(expected.results).foreach{ case (a, e) => a should be (e) }

      case Left(e) =>
        fail(e.toString)
    }
  }

  test("return an error when the expected path to resources does not exist") {
    val body = j"""{}"""
    service.extract(body) match {
      case Right(aggregations) => fail("We should have returned a decode error!")
      case Left(error) => error match {
        case _: DecodeError =>
        case _ => fail("Expected a DecodeError")
      }
    }
  }

  ignore("timings") {}
  ignore("query parameters parser - errors") {}

}


class CountServiceSpecWithTestESData extends FunSuiteLike with Matchers with BeforeAndAfterAll with TestESData {
  val countService = new CountService(documentClient, domainClient, coreClient)

  override protected def beforeAll(): Unit = bootstrapData()

  override protected def afterAll(): Unit = {
    client.close()
    httpClient.close()
  }

  test("categories count request") {
    val expectedResults = List(Count("Personal", 5), Count("It's Personal Buddy",1))
    val (_, res, _, _) = countService.doAggregate(CategoriesFieldType, Map.empty, AuthParams(), None, None)
    res.results should contain theSameElementsAs expectedResults
  }

  test("tags count request") {
    val expectedResults = List(Count("Happy", 5), Count("A Very Happy Capitalized Tag Name",1), Count("Accident", 5))
    val (_, res, _, _) = countService.doAggregate(TagsFieldType, Map.empty, AuthParams(), None, None)
    res.results should contain theSameElementsAs expectedResults
  }

  test("domain categories count request") {
    val expectedResults = List(Count("Alpha to Omega", 2), Count("Alpha", 1), Count("Beta", 2), Count("Gamma", 1),
      Count("Pumas", 1), Count("Fun", 4), Count("Science", 3))
    val (_, res, _, _) = countService.doAggregate(DomainCategoryFieldType, Map.empty, AuthParams(), None, None)
    res.results should contain theSameElementsAs expectedResults
  }

  test("domain tags count request") {
    val expectedResults = List(Count("1-one", 5), Count("2-two", 3), Count("facts",1),
      Count("1-one is only the very first part of this tag",1))
    val (_, res, _, _) = countService.doAggregate(DomainTagsFieldType, Map.empty, AuthParams(), None, None)
    res.results should contain theSameElementsAs expectedResults
  }

  test("owners count request") {
    val expectedResults = List(Count("robin-hood", 5), Count("lil-john", 3),  Count("john-clan", 1), Count("prince-john", 3), Count("honorable.sheriff", 4))
    val (_, res, _, _) = countService.doAggregate(OwnerIdFieldType, Map.empty, AuthParams(), None, None)
    res.results should contain theSameElementsAs expectedResults
  }

  test("throws a DomainNotFoundError when the search_context doesn't exist") {
    val params = Map(
      Params.searchContext -> "bad-domain.com",
      Params.domains -> "petercetera.net,opendata-demo.socrata.com")
      .mapValues(Seq(_))

    intercept[DomainNotFoundError] {
      val (_, res, _, _) = countService.doAggregate(OwnerIdFieldType, params, AuthParams(), None, None)
    }
  }
}

class CountServiceSpecWithBrokenES extends FunSuiteLike with Matchers with MockFactory {
//  ES is broken within this class because it's not Bootstrapped
  val testSuiteName = "BrokenES"
  val client: ElasticSearchClient = new TestESClient(testSuiteName)
  val httpClient = new TestHttpClient()
  val coreClient = new TestCoreClient(httpClient, 8033)
  val domainClient = new DomainClient(client, coreClient, testSuiteName)
  val documentClient = new DocumentClient(client, domainClient, testSuiteName, None, None, Set.empty, None)
  val service = new CountService(documentClient, domainClient, coreClient)

  test("non fatal exceptions throw friendly error string") {
    val expectedResults = """{"error":"We're sorry. Something went wrong."}"""

    val servReq = mock[HttpServletRequest]
    servReq.expects('getHeader)(HeaderAuthorizationKey).anyNumberOfTimes.returns("Basic ricky:awesome")
    servReq.expects('getHeader)(HeaderAuthorizationKey).anyNumberOfTimes.returns("OAuth 123456789")
    servReq.expects('getHeader)(HeaderCookieKey).anyNumberOfTimes.returns("ricky=awesome")
    servReq.expects('getHeader)(HeaderXSocrataHostKey).anyNumberOfTimes.returns("opendata.test")
    servReq.expects('getHeader)(HeaderXSocrataRequestIdKey).anyNumberOfTimes.returns("1")
    servReq.expects('getQueryString)().returns("only=datasets")

    val augReq = new AugmentedHttpServletRequest(servReq)

    val httpReq = mock[HttpRequest]
    httpReq.expects('servletRequest)().anyNumberOfTimes.returning(augReq)

    val response = new MockHttpServletResponse()

    val field = DomainCategoryFieldType

    service.aggregate(field)(httpReq)(response)
    response.getStatus shouldBe SC_INTERNAL_SERVER_ERROR
    response.getHeader("Access-Control-Allow-Origin") shouldBe "*"
    response.getContentAsString shouldBe expectedResults
  }
}
