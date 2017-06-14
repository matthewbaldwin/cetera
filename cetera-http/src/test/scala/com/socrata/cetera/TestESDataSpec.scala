package com.socrata.cetera

import scala.collection.JavaConverters._

import org.elasticsearch.common.settings.Settings
import org.elasticsearch.search.SearchHit
import org.scalatest.{BeforeAndAfterAll, FunSuiteLike, Matchers}

import com.socrata.cetera.search.ElasticSearchClient
import com.socrata.cetera.types._

class TestESDataSpec extends FunSuiteLike with Matchers with TestESData with BeforeAndAfterAll {

  override protected def beforeAll(): Unit = bootstrapData()

  override protected def afterAll(): Unit = {
    removeBootstrapData()
    client.close()
  }

  // stringifiers in case of debugging elasticsearch settings, mappings, documents, et cetera
  def formatEsSettings(settingsMap: Iterable[(String, Settings)]): String = {
    settingsMap.map { case (index: String, settings: Settings) =>
      (index, settings.getAsStructuredMap)
    }.mkString("\n")
  }

  def formatEsHits(hits: Array[SearchHit]): String = {
    hits.toList.map(_.getSourceAsString()).mkString("\n")
  }

  test("aliases are bootstrapped") {
    val res = client.client.admin.indices.prepareGetAliases()
      .execute.actionGet
    val indexAliases = res.getAliases.asScala.map(ooc => ooc.key -> ooc.value.asScala.map(_.alias))
    indexAliases.size should be(1) // number of indices
    indexAliases.flatMap(as => as._2).size should be(1) // number of aliases
    indexAliases.find { case (_,as) => as.contains(testSuiteName) } should be('defined)
  }

  test("settings are bootstrapped") {
    val res = client.client.admin.indices.prepareGetSettings(testSuiteName)
      .execute.actionGet
    val settings = res.getIndexToSettings.asScala.map(ooc => ooc.key -> ooc.value)
    settings.size should be(1)
  }

  test("mappings are bootstrapped") {
    val res = client.client.admin().indices().prepareGetMappings()
      .execute.actionGet
    val mappings = res.getMappings.asScala.flatMap { ooc =>
      ooc.value.asScala.map { iom =>
        iom.key -> iom.value.sourceAsMap().asScala
      }
    }
    mappings.size should be(3)
  }

  test("test domains are bootstrapped") {
    val res = client.client.prepareSearch().setTypes(esDomainType).execute.actionGet
    val numDomains = domains.length
    res.getHits.getTotalHits should be(numDomains)
  }

  test("test documents are bootstrapped") {
    val res = client.client.prepareSearch().setTypes(esDocumentType).execute.actionGet
    val numDocs = docs.length
    res.getHits.getTotalHits should be(numDocs)
  }

  test("test users are bootstrapped") {
    val res = client.client.prepareSearch().setTypes(esUserType).execute.actionGet
    val numUsers = users.length
    res.getHits.getTotalHits should be(numUsers)
  }
}
