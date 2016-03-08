package com.socrata.cetera.services

import org.scalatest.{BeforeAndAfterAll, FunSuiteLike, Matchers}

import com.socrata.cetera.{TestESData, TestESClient}
import com.socrata.cetera.search._
import com.socrata.cetera.types.Datatypes

class FacetServiceSpec extends FunSuiteLike with Matchers with TestESData with BeforeAndAfterAll {
  val client: ElasticSearchClient = new TestESClient(testSuiteName)
  val domainClient: DomainClient = new DomainClient(client, testSuiteName)
  val documentClient: DocumentClient = new DocumentClient(client, testSuiteName, None, None, Set.empty)
  val service: FacetService = new FacetService(documentClient)

  override protected def beforeAll(): Unit = {
    bootstrapData()
  }

  override protected def afterAll(): Unit = {
    removeBootstrapData()
    client.close()
  }

  test("retrieve all domain facets") {
    val (facets, timings) = service.doAggregate("")

    timings.searchMillis should be('defined)

    val datatypes = facets.find(_.facet == "datatypes").map(_.values).getOrElse(fail())
    Datatypes.materialized.foreach { dt =>
      datatypes.find(_.value == dt.singular) should be('defined)
    }

    val categories = facets.find(_.facet == "categories").map(_.values).getOrElse(fail())
    categories.find(_.value == "") shouldNot be('defined)
    domainCategories.distinct.filter(_.nonEmpty).foreach { cat =>
      categories.find(_.value == cat) should be('defined)
    }

    val tags = facets.find(_.facet == "tags").map(_.values).getOrElse(fail())
    tags.find(_.value == "") shouldNot be('defined)
    domainTags.flatten.distinct.filter(_.nonEmpty).foreach { tag =>
      tags.find(_.value == tag) should be('defined)
    }

    domainMetadata.flatMap(_.keys).distinct.foreach { key =>
      val facet = facets.find(_.facet == key)
      facet should be('defined)
      facet.get.count should be(facet.get.values.map(_.count).sum)
    }
  }
}
