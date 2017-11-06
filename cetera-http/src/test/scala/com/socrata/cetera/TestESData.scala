package com.socrata.cetera

import java.io.File
import scala.io.Source

import com.rojoma.json.v3.ast.JValue
import com.rojoma.json.v3.interpolation._
import com.rojoma.json.v3.io.CompactJsonWriter
import com.rojoma.json.v3.util.JsonUtil
import org.elasticsearch.action.support.WriteRequest.RefreshPolicy
import org.elasticsearch.common.xcontent.XContentType
import org.mockserver.integration.ClientAndServer._
import org.mockserver.model.HttpRequest.request

import com.socrata.cetera.auth.AuthParams
import com.socrata.cetera.metrics.BalboaClient
import com.socrata.cetera.response.{SearchResult, SearchResults}
import com.socrata.cetera.search.{DocumentClient, DomainClient}
import com.socrata.cetera.services.{AutocompleteService, SearchService}
import com.socrata.cetera.types._
import com.socrata.cetera.util.ElasticsearchBootstrap

/*
This trait makes available just about every combination of document/domain/user you might want for testing.
For a summary of the domains that are available, see either test/resources/domains.tsv or look at the DomainSpec
For a summary of the views that are available, see either test/resources/views/ or look at the DocumentSpec
*/
trait TestESData extends TestESDomains with TestESUsers {
  val testSuiteName: String = getClass.getSimpleName.toLowerCase
  val mockServer = startClientAndServer(0)
  val coreTestPort = mockServer.getPort

  val client = new TestESClient(testSuiteName)
  val httpClient = new TestHttpClient()
  val coreClient = new TestCoreClient(httpClient, coreTestPort)

  val domainClient = new DomainClient(client, coreClient, testSuiteName)
  val documentClient = new DocumentClient(client, domainClient, testSuiteName, None, None, Set.empty, None)

  val balboaDir = new File(s"/tmp/metrics/cetera/$testSuiteName")
  balboaDir.delete()
  balboaDir.mkdirs()

  val balboaClient = new BalboaClient(balboaDir.getAbsolutePath)

  val service = new SearchService(documentClient, domainClient, balboaClient, coreClient)
  val autocompleteService = new AutocompleteService(documentClient, domainClient, coreClient)
  val cookie = "C = Cookie"
  val superAdminBody = j"""{"id" : "who-am-i", "flags" : [ "admin" ]}"""
  val allDomainsParams = Map(
    "domains" -> domains.map(_.domainCname).mkString(","),
    "show_visibility" -> "true"
  ).mapValues(Seq(_))

  val domDocCount = Map(0->9, 1->2, 2->7, 3->9, 8->0 ,9->4)
  val docs = {
    val files = domDocCount.map { case (dom: Int, docCount: Int) =>
      (0 to docCount).map(i => s"/views/d$dom-v$i.json")
    }.flatten

    files.map { f =>
      val source = Source.fromInputStream(getClass.getResourceAsStream(f)).getLines().mkString("\n")
      Document(source).get
    }
  }.toList

  val anonymouslyViewableDocIds = List(
    "d0-v0", "d1-v0", "d0-v2", "d2-v2", "d3-v2", "d3-v3", "d0-v3", "d0-v4", "d3-v4",
    "d2-v4", "d0-v7", "d0-v9", "d9-v0", "d9-v1", "d9-v2", "d9-v3", "d9-v4")
  val (anonymouslyViewableDocs, internalDocs) =
    docs.partition(d => anonymouslyViewableDocIds contains(d.socrataId.datasetId))

  def bootstrapData(): Unit = {
    ElasticsearchBootstrap.ensureIndex(client, "yyyyMMddHHmm", testSuiteName)

    // load domains
    domains.foreach { d =>
      client.client.prepareIndex(testSuiteName, esDomainType)
        .setSource(JsonUtil.renderJson[Domain](d), XContentType.JSON)
        .setId(d.domainId.toString)
        .setRefreshPolicy(RefreshPolicy.IMMEDIATE)
        .execute.actionGet
    }

    // load users
    users.foreach { u =>
      client.client.prepareIndex(testSuiteName, esUserType)
        .setSource(JsonUtil.renderJson[EsUser](u), XContentType.JSON)
        .setId(u.id)
        .setRefreshPolicy(RefreshPolicy.IMMEDIATE)
        .execute.actionGet
    }

    // load data
    docs.foreach { d =>
      client.client.prepareIndex(testSuiteName, esDocumentType)
        .setId(d.socrataId.datasetId)
        .setParent(d.socrataId.domainId.toString)
        .setSource(JsonUtil.renderJson(d), XContentType.JSON)
        .setRefreshPolicy(RefreshPolicy.IMMEDIATE)
        .execute.actionGet
    }
  }

  def removeBootstrapData(): Unit = {
    client.client.admin().indices().prepareDelete(testSuiteName)
      .execute.actionGet
  }

  def authedUserBodyFromRole(role: String, id: String = "cook-mons"): JValue = {
    j"""{
          "id" : $id,
          "roleName" : $role,
          "rights" : [ "eat_cookies", "spell_words_starting_with_c" ]
        }"""
  }

  // Regarding roles in the two following methods:
  //   roles are only relevant in domain search - for including locked domains
  //   and for user search - to return all roled-domain users.
  //   roles have no bearing on document search, so rather than test every single combination
  //   of roles rights, we focus on 2 practical cases here:
  //     - users with rights, who must then have a role
  //     - users with neither.
  def authedUserBodyWithRoleAndRights(rights: Seq[String], id: String = "cook-mons"): JValue = j"""
    {
      "id" : $id,
      "roleName" : "role",
      "rights" : $rights
    }"""

  def authedUserBodyWithoutRoleOrRights(id: String = "cook-mons"): JValue = j"""
    {
      "id" : $id
    }"""

  def prepareAuthenticatedUser(cookie: String, host: String, userJson: JValue): Unit = {
    val expectedRequest = request()
      .withMethod("GET")
      .withPath("/users.json")
      .withHeader(HeaderXSocrataHostKey, host)
      .withHeader(HeaderCookieKey, cookie)

    // response is hidden by our own "response" directory
    val expectedResponse = org.mockserver.model.HttpResponse.response()
      .withStatusCode(200)
      .withHeader("Content-Type", "application/json; charset=utf-8")
      .withBody(CompactJsonWriter.toString(userJson))

    mockServer
      .when(expectedRequest)
      .respond(expectedResponse)
  }

  def getAllPossibleResults(): Seq[SearchResult] = {
    val host = domains(0).domainCname
    val authedUserBody = j"""{"id" : "who-am-i", "flags" : [ "admin" ]}"""
    val adminCookie = "admin=super"
    prepareAuthenticatedUser(adminCookie, host, authedUserBody)
    service.doSearch(allDomainsParams, AuthParams(cookie=Some(adminCookie)), Some(host), None)._2.results
  }

  def fxfs(searchResult: SearchResult): String = searchResult.resource.id

  def fxfs(searchResults: SearchResults[SearchResult]): Seq[String] =
    searchResults.results.map(fxfs)

  def fxfs(docs: Seq[Document]): Seq[String] = docs.map(_.socrataId.datasetId).toSet.toSeq

  def emptyAndRemoveDir(dir: File): Unit = {
    if (dir.isDirectory) {
      dir.listFiles().foreach(f => f.delete())
    }
    dir.delete()
  }

  def isApproved(res: SearchResult): Boolean = {
    val raApproved = res.metadata.isRoutingApproved.map(identity(_)).getOrElse(true) // None implies not relevant (so approved by default)
    val vmApproved = res.metadata.isModerationApproved.map(identity(_)).getOrElse(true)
    raApproved && vmApproved
  }
}
