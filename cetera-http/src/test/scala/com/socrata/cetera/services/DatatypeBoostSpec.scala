package com.socrata.cetera.services

import com.rojoma.json.v3.ast.{JNumber, JString}
import org.scalatest.{BeforeAndAfterAll, FunSuiteLike, Matchers}

import com.socrata.cetera.{TestHttpClient, TestCoreClient, TestESData, TestESClient}
import com.socrata.cetera.metrics.BalboaClient
import com.socrata.cetera.search._
import com.socrata.cetera.types._
import com.socrata.cetera.util.Params

class DatatypeBoostSpec extends FunSuiteLike with Matchers with TestESData with BeforeAndAfterAll {
  val boostedDatatype = TypeCalendars
  val boostedDatatypeQueryString = "boost" + boostedDatatype.plural.capitalize

  val client = new TestESClient(testSuiteName)
  val httpClient = new TestHttpClient()
  val coreClient = new TestCoreClient(httpClient, 8033)
  val domainClient = new DomainClient(client, coreClient, testSuiteName)
  val documentClient = new DocumentClient(client, domainClient, testSuiteName, None, None, Set.empty)
  val balboaClient = new BalboaClient("/tmp/metrics")
  val service = new SearchService(documentClient, domainClient, balboaClient)

  override protected def beforeAll(): Unit = {
    bootstrapData()
  }

  override protected def afterAll(): Unit = {
    removeBootstrapData()
    client.close()
    httpClient.close()
  }

  test("datatype boost - increases score when datatype matches") {
    val (results, _) = service.doSearch(Map(
      Params.context -> "petercetera.net",
      Params.filterDomains -> "petercetera.net,opendata-demo.socrata.com,blue.org,annabelle.island.net",
      Params.querySimple -> "one",
      Params.showScore -> "true",
      boostedDatatypeQueryString -> "10"
    ).mapValues(Seq(_)), None)
    val oneBoosted = results.results.find(_.resource.dyn.`type`.! == JString(boostedDatatype.singular)).head
    val oneOtherThing = results.results.find(_.resource.dyn.`type`.! != JString(boostedDatatype.singular)).head

    val datalensScore: Float = oneBoosted.metadata("score") match {
      case n: JNumber => n.toFloat
      case _ => fail()
    }
    val otherScore: Float = oneOtherThing.metadata("score") match {
      case n: JNumber => n.toFloat
      case _ => fail()
    }

    datalensScore should be > otherScore
  }
}