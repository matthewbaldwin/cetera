package com.socrata.cetera.services

import java.nio.charset.{Charset, CodingErrorAction}

import com.rojoma.json.v3.ast.JString
import org.scalatest.{BeforeAndAfterAll, BeforeAndAfterEach, FunSuiteLike, Matchers}

import com.socrata.cetera.TestESData
import com.socrata.cetera.auth.AuthParams
import com.socrata.cetera.errors.MissingRequiredParameterError
import com.socrata.cetera.handlers.Params
import com.socrata.cetera.response.SearchResult
import com.socrata.cetera.response.{CompletionResult, SearchResults}

class AutocompleteServiceSpec
  extends FunSuiteLike
    with Matchers
    with TestESData
    with BeforeAndAfterAll
    with BeforeAndAfterEach {

  override protected def beforeAll(): Unit = bootstrapData()

  override protected def afterAll(): Unit = {
    removeBootstrapData()
    client.close()
    httpClient.close()
  }

  test("an autocomplete search without a search query throws") {
    intercept[MissingRequiredParameterError] {
      val basicDomain = domains(0).domainCname
      val params = Map("search_context" -> basicDomain, "domains" -> basicDomain).mapValues(Seq(_))
      autocompleteService.doSearch(params, requireAuth = false, AuthParams(), None, None)._2
    }
  }

  test("an autocomplete search with mulitple terms works as expected") {
    val params = Map("q" -> "Multiword Title").mapValues(Seq(_))
    val SearchResults(actualCompletions, _, _) = autocompleteService.doSearch(
      params, requireAuth = false, AuthParams(), None, None)._2
    val expectedCompletions = List(CompletionResult("A Multiword Title", "A <span class=highlight>Multiword</span> <span class=highlight>Title</span>"))
    actualCompletions should contain theSameElementsAs expectedCompletions
  }

  test("an autocomplete search restricted to a domain should return the expected results") {
    val basicDomain = domains(0).domainCname
    val params = Map("search_context" -> basicDomain, "domains" -> basicDomain, "q" -> "o")
      .mapValues(Seq(_))
    val SearchResults(actualCompletions, _, _) = autocompleteService.doSearch(
      params, requireAuth = false, AuthParams(), None, None)._2
    val expectedCompletions = List(CompletionResult("One", "<span class=highlight>O</span>ne"))
    actualCompletions should contain theSameElementsAs expectedCompletions
  }
}
