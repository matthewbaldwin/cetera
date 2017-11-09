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
    "domains" -> domains.map(_.cname).mkString(","),
    "show_visibility" -> "true"
  ).mapValues(Seq(_))

  def docsFromFiles(d: Int, v: Int): List[Document] =
    (0 to v).map(i => s"/views/d$d-v$i.json").map { f =>
      val source = Source.fromInputStream(getClass.getResourceAsStream(f)).getLines().mkString("\n")
      Document(source).get
    }.toList

  val dom0Docs = docsFromFiles(0, 9)
  val dom1Docs = docsFromFiles(1, 4)
  val dom2Docs = docsFromFiles(2, 7)
  val dom3Docs = docsFromFiles(3, 9)
  val dom8Docs = docsFromFiles(8, 0)
  val dom9Docs = docsFromFiles(9, 4)

  val docs = dom0Docs ++ dom1Docs ++ dom2Docs ++ dom3Docs ++ dom8Docs ++ dom9Docs

  val anonymouslyViewableDocIds = List(
    "d0-v0", "d0-v2", "d0-v3", "d0-v4", "d0-v7", "d0-v9",
    "d1-v0", "d1-v3", "d1-v4",
    "d2-v2", "d2-v4",
    "d3-v2", "d3-v3", "d3-v4",
    "d9-v0", "d9-v1", "d9-v2", "d9-v3", "d9-v4"
  )

  val (anonymouslyViewableDocs, internalDocs) =
    docs.partition(d => anonymouslyViewableDocIds contains(d.socrataId.datasetId))

  def docsForDomain(domain: Domain) = {
    domain.id match {
      case 0 => dom0Docs
      case 1 => dom1Docs
      case 2 => dom2Docs
      case 3 => dom3Docs
      case 9 => dom9Docs
      case _ => docs
    }
  }

  def isVisibleToUser(doc: Document, userId: String) =
    doc.isSharedOrOwned(userId) || anonymouslyViewableDocIds.contains(doc.socrataId.datasetId)

  def bootstrapData(): Unit = {
    ElasticsearchBootstrap.ensureIndex(client, "yyyyMMddHHmm", testSuiteName)

    // load domains
    domains.foreach { d =>
      client.client.prepareIndex(testSuiteName, esDomainType)
        .setSource(JsonUtil.renderJson[Domain](d), XContentType.JSON)
        .setId(d.id.toString)
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

  def fxfs(searchResults: SearchResults[SearchResult]): List[String] =
    searchResults.results.map(_.resource.id).toList.sorted

  def fxfs(docs: Seq[Document]): List[String] = docs.map(_.socrataId.datasetId).toList.sorted

  def emptyAndRemoveDir(dir: File): Unit = {
    if (dir.isDirectory) {
      dir.listFiles().foreach(f => f.delete())
    }
    dir.delete()
  }
}
