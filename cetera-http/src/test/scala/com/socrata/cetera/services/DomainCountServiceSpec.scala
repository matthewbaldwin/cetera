package com.socrata.cetera.services

import javax.servlet.http.HttpServletRequest

import com.rojoma.json.v3.ast.{JNumber, JValue}
import com.socrata.http.server.HttpRequest
import com.socrata.http.server.HttpRequest.AugmentedHttpServletRequest
import org.apache.http.HttpStatus.SC_INTERNAL_SERVER_ERROR
import org.scalamock.scalatest.proxy.MockFactory
import org.scalatest.{BeforeAndAfterAll, FunSuiteLike, Matchers, ShouldMatchers, WordSpec}
import org.springframework.mock.web.MockHttpServletResponse

import com.socrata.cetera._
import com.socrata.cetera.auth.AuthParams
import com.socrata.cetera.errors.DomainNotFoundError
import com.socrata.cetera.handlers.Params
import com.socrata.cetera.search._
import com.socrata.cetera.types.Count

class DomainCountServiceSpec extends WordSpec with ShouldMatchers with BeforeAndAfterAll with TestESData {
  val domainCountService = new DomainCountService(domainClient, coreClient)
  val docCountsByDomain = anonymouslyViewableDocs.groupBy(_.socrataId.domainId).mapValues(_.size)

  override protected def beforeAll(): Unit = bootstrapData()

  override protected def afterAll(): Unit = {
    client.close()
    httpClient.close()
  }

  private def countToInt(c: Count): Int = c.count.dyn.! match {
    case jn: JNumber => jn.toInt
    case jv: JValue => throw new RuntimeException(s"Unexpected json value $jv")
  }

  def haveMatchingDocAndDomSearchCounts(params: Map[String, String]): Unit = {
    val paramVals = params.values.mkString(",")
    s"have matching docSearch counts and domSearch counts with the values $paramVals" in {
      val (_, domRes, _, _) = domainCountService.doAggregate(params.mapValues(Seq(_)), AuthParams(), None, None)
      val docSearch = service.doSearch(params.mapValues(Seq(_)), AuthParams(), None, None)
      val docSearchCount = docSearch._2.resultSetSize
      val domSearchCount = domRes.results.foldLeft(0)((sum, count) => sum + countToInt(count))
      domSearchCount should be(docSearchCount)
      domSearchCount should not be(0)
    }
  }

  def haveMatchingDocAndDomSearchCountsWhenAuthed(params: Map[String, String], host: String): Unit = {
    val paramVals = params.values.mkString(",")
    s"have matching docSearch counts and domSearch counts with the values $paramVals" in {
      prepareAuthenticatedUser(cookie, host, superAdminBody)
      val (_, domRes, _, _) = domainCountService.doAggregate(params.mapValues(Seq(_)), AuthParams(cookie=Some(cookie)), Some(host), None)
      prepareAuthenticatedUser(cookie, host, superAdminBody)
      val docSearch = service.doSearch(params.mapValues(Seq(_)), AuthParams(cookie=Some(cookie)), Some(host), None)
      val docSearchCount = docSearch._2.resultSetSize
      val domSearchCount = domRes.results.foldLeft(0)((sum, count) => sum + countToInt(count))
      domSearchCount should be(docSearchCount)
      domSearchCount should not be(0)
    }
  }

  "counting documents by domains when no params are passed - and thus hitting up customer domains" should {
    "have the correct counts" in {
      val expectedResults = List(
        Count(domains(0).domainCname, docCountsByDomain.getOrElse(0, 0)),
        Count(domains(2).domainCname, docCountsByDomain.getOrElse(2, 0)),
        Count(domains(3).domainCname, docCountsByDomain.getOrElse(3, 0)),
        Count(domains(4).domainCname, docCountsByDomain.getOrElse(4, 0)))
      val (_, res, _, _) = domainCountService.doAggregate(Map.empty, AuthParams(), None, None)
      res.results should contain theSameElementsAs expectedResults
    }
  }

  "counting documents by domains when allDomainsParams are passed - noting that locked domains will not be returned b/c of lack of auth" should {
    "have the correct counts" in {
      val expectedResults = List(
        Count(domains(0).domainCname, docCountsByDomain.getOrElse(0, 0)),
        Count(domains(1).domainCname, docCountsByDomain.getOrElse(1, 0)),
        Count(domains(2).domainCname, docCountsByDomain.getOrElse(2, 0)),
        Count(domains(3).domainCname, docCountsByDomain.getOrElse(3, 0)),
        Count(domains(4).domainCname, docCountsByDomain.getOrElse(4, 0)),
        Count(domains(5).domainCname, docCountsByDomain.getOrElse(5, 0)))
      val (_, res, _, _) = domainCountService.doAggregate(allDomainsParams, AuthParams(), None, None)
      res.results should contain theSameElementsAs expectedResults
    }
  }

  "counting documents by domains when an unmoderated, R&A-disabled search context is given - which should match the regular domain counts as no additional filtering is happening" should {
    "have the correct counts" in {
      val contextId = 0
      val context = domains(contextId).domainCname
      val expectedResults = List(
        Count(context, docCountsByDomain.getOrElse(contextId, 0)),
        Count(domains(2).domainCname, docCountsByDomain.getOrElse(2, 0)),
        Count(domains(3).domainCname, docCountsByDomain.getOrElse(3, 0)))
      val (_, res, _, _) = domainCountService.doAggregate(Map(
        Params.searchContext -> context,
        Params.domains -> s"$context,blue.org,annabelle.island.net")
        .mapValues(Seq(_)), AuthParams(), None, None)
      res.results should contain theSameElementsAs expectedResults
    }
  }

  "counting documents by domains when an unmoderated and R&A-enabled search context - which will reduce results since only approved-by-the-context views will federate" should {
    "have the correct counts" in {
      val contextId = 2
      val context = domains(contextId).domainCname
      val expectedResults = List(
        Count(context, docCountsByDomain.getOrElse(contextId, 0)), // the context's counts should stay the same
        Count("petercetera.net", 3)) // the federated domain only has 3 anon-visible views approved by the context: zeta-0001, zeta-0007, zeta-0012,
      val (_, res, _, _) = domainCountService.doAggregate(Map(
          Params.searchContext -> context,
          Params.domains -> s"$context,petercetera.net")
          .mapValues(Seq(_)), AuthParams(), None, None)

      res.results should contain theSameElementsAs expectedResults
    }
  }

  "counting documents by domains when a moderated and R&A-disabled search context - which will reduce results since only default views will federate" should {
    "have the correct counts" in {
      val contextId = 1
      val context = domains(contextId).domainCname
      val expectedResults = List(
        Count("petercetera.net", 2), // the federated domain only has 2 anon-visible default views: fxf-8 and zeta-0007
        Count(context, docCountsByDomain.getOrElse(contextId, 0))) // the context's counts should stay the same
      val (_, res, _, _) = domainCountService.doAggregate(Map(
          Params.searchContext -> context,
          Params.domains -> s"$context,petercetera.net")
          .mapValues(Seq(_)), AuthParams(), None, None)

      res.results should contain theSameElementsAs expectedResults
    }
  }

  "counting documents by domains when a moderated and R&A-enabled search context - which will reduce results since only default approved-by-the-context views will federate" should {
    "have the correct counts" in {
      val contextId = 3
      val context = domains(contextId).domainCname
      val expectedResults = List(
        Count(context, docCountsByDomain.getOrElse(contextId, 0)), // the context's counts should stay the same
        Count("blue.org", 1), // blue.org has 1 qualifying dataset: fxf-10
        Count("petercetera.net", 0)) // petercetera.net has no qualifying datasets
      val (_, res, _, _) = domainCountService.doAggregate(Map(
          Params.searchContext -> context,
          Params.domains -> s"$context,petercetera.net,blue.org")
          .mapValues(Seq(_)), AuthParams(), None, None)

      res.results should contain theSameElementsAs expectedResults
    }
  }

  // ------------------------------------------------------------------------------------------------
  // the series of tests below check that the counts returned from document search match those returned
  // from domainCount search. The params that are excluded here include:
  //   the search_context: since that is tested in greater detail above
  //   the q param: since that isn't supported at all and we expect counts to differ
  // ------------------------------------------------------------------------------------------------
  "counting documents by domains with the domains param" should {
    haveMatchingDocAndDomSearchCounts(Map(Params.domains -> domains(0).domainCname))
  }

  "counting documents by domains with a custom metadata param" should {
    haveMatchingDocAndDomSearchCounts(Map("one" -> "1"))
  }

  "counting documents by domains with only the categories param - thus using socrata categories" should {
    haveMatchingDocAndDomSearchCounts(Map(Params.categories -> "Personal"))
  }

  "counting documents by domains with the categories and search context params - thus using customer categories" should {
    haveMatchingDocAndDomSearchCounts(Map(Params.categories -> "Alpha to Omega", Params.searchContext -> domains(0).domainCname))
  }

  "counting documents by domains with only the tags param - thus using socrata tags" should {
    haveMatchingDocAndDomSearchCounts(Map(Params.tags -> "Happy"))
  }

  "counting documents by domains with the tags and search context params  - thus using customer tags" should {
    haveMatchingDocAndDomSearchCounts(Map(Params.tags -> "1-one", Params.searchContext -> domains(0).domainCname))
  }

  "counting documents by domains with the datatypes param" should {
    haveMatchingDocAndDomSearchCounts(Map(Params.only -> "datasets"))
  }

  "counting documents by domains with the user param" should {
    haveMatchingDocAndDomSearchCounts(Map(Params.forUser -> "robin-hood"))
  }

  "counting documents by domains with the attribution param" should {
    haveMatchingDocAndDomSearchCounts(Map(Params.attribution -> "nottingham"))
  }

  "counting documents by domains with the provenance param" should {
    haveMatchingDocAndDomSearchCounts(Map(Params.provenance -> "community"))
    haveMatchingDocAndDomSearchCounts(Map(Params.provenance -> "official"))
  }

  "counting documents by domains with the parentDatasetId param" should {
    haveMatchingDocAndDomSearchCounts(Map(Params.derivedFrom -> "fxf-0"))
  }

  "counting documents by domains with the ids param" should {
    haveMatchingDocAndDomSearchCounts(Map(Params.ids -> "fxf-10"))
  }

  "counting documents by domains with the license param" should {
    haveMatchingDocAndDomSearchCounts(Map(Params.license -> "Academic Free License"))
  }

  "counting documents by domains with the sharedTo param" should {
    haveMatchingDocAndDomSearchCountsWhenAuthed(Map(Params.sharedTo -> "lil-john"), domains(0).domainCname)
  }

  "counting documents by domains with the public param" should {
    haveMatchingDocAndDomSearchCountsWhenAuthed(Map(Params.public -> "false"), domains(0).domainCname)
    haveMatchingDocAndDomSearchCountsWhenAuthed(Map(Params.public -> "true"), domains(0).domainCname)
  }

  "counting documents by domains with the published param" should {
    haveMatchingDocAndDomSearchCountsWhenAuthed(Map(Params.published -> "false"), domains(0).domainCname)
    haveMatchingDocAndDomSearchCountsWhenAuthed(Map(Params.published -> "true"), domains(0).domainCname)
  }

  "counting documents by domains with the approvalStatus param" should {
    haveMatchingDocAndDomSearchCountsWhenAuthed(Map(Params.approvalStatus -> "pending"), domains(0).domainCname)
    haveMatchingDocAndDomSearchCountsWhenAuthed(Map(Params.approvalStatus -> "rejected"), domains(0).domainCname)
    haveMatchingDocAndDomSearchCountsWhenAuthed(Map(Params.approvalStatus -> "approved"), domains(0).domainCname)
  }

  "counting documents by domains with the explicitlyHidden param" should {
    haveMatchingDocAndDomSearchCountsWhenAuthed(Map(Params.explicitlyHidden -> "false"), domains(0).domainCname)
    haveMatchingDocAndDomSearchCountsWhenAuthed(Map(Params.explicitlyHidden -> "true"), domains(0).domainCname)
  }
  // ------------------------------------------------------------------------------------------------

  "counting documents by domains with a bucket of search params - noting the lack of search context and use of socrata cats/tags" should {
    "have the correct counts " in {
      val context = "petercetera.net"
      val expectedResults = List(Count(context, 1)) // this describes fxf-0 and only fxf-0
      val (_, res, _, _) = domainCountService.doAggregate(Map(
          Params.domains -> context,
          Params.only -> "calendars",
          "one" -> "1", // custom metadata,
          Params.categories -> "Personal", // socrata category
          Params.tags -> "Happy", // socrata tag
          Params.forUser -> "robin-hood")
          .mapValues(Seq(_)), AuthParams(), None, None)

      res.results should contain theSameElementsAs expectedResults
    }
  }

  "counting documents by domains with a bucket of search params - noting the presence of a search context and customer cats/tags" should {
    "have the correct counts " in {
      val context = "petercetera.net"
      val expectedResults = List(Count(context, 1)) // this describes fxf-0 and only fxf-0
      val (_, res, _, _) = domainCountService.doAggregate(Map(
          Params.searchContext -> context,
          Params.domains -> context,
          Params.only -> "calendars",
          "one" -> "1",  // custom metadata,
          Params.categories -> "Alpha to Omega", // customer category
          Params.tags -> "1-one", // customer tag
          Params.forUser -> "robin-hood")
          .mapValues(Seq(_)), AuthParams(), None, None)

      res.results should contain theSameElementsAs expectedResults
    }
  }

  "counting documents by domains with a non-existent search_context" should {
    "throws a DomainNotFoundError" in {
      val params = Map(
        Params.searchContext -> "bad-domain.com",
        Params.domains -> "petercetera.net,opendata-demo.socrata.com")
        .mapValues(Seq(_))

      intercept[DomainNotFoundError] {
        val (_, res, _, _) = domainCountService.doAggregate(params, AuthParams(), None, None)
      }
    }
  }
}

class DomainCountServiceSpecWithBrokenES extends FunSuiteLike with Matchers with MockFactory {
  //  ES is broken within this class because it's not Bootstrapped
  val testSuiteName = "BrokenES"
  val client = new TestESClient(testSuiteName)
  val httpClient = new TestHttpClient()
  val coreClient = new TestCoreClient(httpClient, 8035)
  val domainClient = new DomainClient(client, coreClient, testSuiteName)
  val service = new DomainCountService(domainClient, coreClient)

  test("non fatal exceptions throw friendly error string") {
    val expectedResults = """{"error":"We're sorry. Something went wrong."}"""

    val servReq = mock[HttpServletRequest]
    servReq.expects('getHeader)(HeaderAuthorizationKey).anyNumberOfTimes.returns("Basic ricky:awesome")
    servReq.expects('getHeader)(HeaderCookieKey).anyNumberOfTimes.returns("ricky=awesome")
    servReq.expects('getHeader)(HeaderXSocrataHostKey).anyNumberOfTimes.returns("opendata.test")
    servReq.expects('getHeader)(HeaderXSocrataRequestIdKey).anyNumberOfTimes.returns("1")
    servReq.expects('getQueryString)().returns("only=datasets")

    val augReq = new AugmentedHttpServletRequest(servReq)

    val httpReq = mock[HttpRequest]
    httpReq.expects('servletRequest)().anyNumberOfTimes.returning(augReq)

    val response = new MockHttpServletResponse()

    service.aggregate()(httpReq)(response)
    response.getStatus shouldBe SC_INTERNAL_SERVER_ERROR
    response.getHeader("Access-Control-Allow-Origin") shouldBe "*"
    response.getContentAsString shouldBe expectedResults
  }
}
