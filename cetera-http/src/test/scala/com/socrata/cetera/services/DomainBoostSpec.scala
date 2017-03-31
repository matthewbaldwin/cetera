package com.socrata.cetera.services

import com.rojoma.json.v3.ast.JString
import org.scalatest.{BeforeAndAfterAll, FunSuiteLike, Matchers}

import com.socrata.cetera._
import com.socrata.cetera.auth.AuthParams
import com.socrata.cetera.handlers.Params
import com.socrata.cetera.metrics.BalboaClient
import com.socrata.cetera.search._

class DomainBoostSpec extends FunSuiteLike with Matchers with TestESData with BeforeAndAfterAll {
  override protected def beforeAll(): Unit = bootstrapData()

  override protected def afterAll(): Unit = {
    removeBootstrapData()
    client.close()
    httpClient.close()
  }

  val activeDomainCnames = Seq("petercetera.net", "blue.org")

  test("domain boost of 2 should produce top result") {
    activeDomainCnames.foreach { domain =>
      val (_, results, _, _) = service.doSearch(Map(
        Params.showScore -> "true",
        Params.searchContext -> domain,
        Params.domains -> activeDomainCnames.mkString(","),
        s"""boostDomains[${domain}]""" -> "2").mapValues(Seq(_)), AuthParams(), None, None)
      results.results.head.metadata.domain should be(domain)
    }
  }

  test("domain boost of 0.5 should not produce top result") {
    activeDomainCnames.foreach { domain =>
      val (_, results, _, _) = service.doSearch(Map(
        Params.showScore -> "true",
        Params.searchContext -> domain,
        Params.domains -> activeDomainCnames.mkString(","),
        s"""boostDomains[${domain}]""" -> "0.5").mapValues(Seq(_)), AuthParams(), None, None)
      results.results.head.metadata.domain shouldNot be(JString(domain))
    }
  }

  test("domain boosts are not mistaken for custom metadata") {
    activeDomainCnames.foreach { domain =>
      val (_, results, _, _) = service.doSearch(
        Map(s"""boostDomains[${domain}]""" -> "2.34",
            "boostDomains[]" -> "3.45", // if I were custom metadata, I would not match any documents
            Params.searchContext -> domain).mapValues(Seq(_)), AuthParams(), None, None
      )
      results.results.size should be > 0
    }
  }

  // just documenting behavior; this may not be ideal behavior
  test("boostDomains with no [] is treated as custom metadata") {
    activeDomainCnames.foreach { domain =>
      val (_, results, _, _) = service.doSearch(
        Map(s"""boostDomains[${domain}]""" -> "2.34",
            "boostDomains" -> "3.45", // interpreted as custom metadata field which doesn't match any documents
            Params.searchContext -> domain).mapValues(Seq(_)), AuthParams(), None, None
      )

      results.results should contain theSameElementsAs List.empty
    }
  }
}
