package com.socrata.cetera.services

import java.nio.charset.{Charset, CodingErrorAction}

import com.rojoma.json.v3.ast.JString
import org.scalatest.{BeforeAndAfterAll, BeforeAndAfterEach, FunSuiteLike, Matchers}

import com.socrata.cetera.{TestESData, TestESDomains}
import com.socrata.cetera.auth.AuthParams
import com.socrata.cetera.errors.MissingRequiredParameterError
import com.socrata.cetera.handlers.Params
import com.socrata.cetera.response.{CompletionResult, MatchSpan, SearchResult, SearchResults}

class AutocompleteServiceSpec
  extends FunSuiteLike
    with Matchers
    with TestESData
    with TestESDomains
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
      val basicDomain = domains(0).cname
      val params = Map("search_context" -> basicDomain, "domains" -> basicDomain).mapValues(Seq(_))
      autocompleteService.doSearch(params, AuthParams(), None, None)._2
    }
  }

  test("an autocomplete search with mulitple terms works as expected") {
    val params = Map("q" -> "Multiword Title").mapValues(Seq(_))
    val SearchResults(actualCompletions, _, _) = autocompleteService.doSearch(
      params, AuthParams(), None, None)._2
    val expectedCompletions = List(CompletionResult("A Multiword Title", "A <span class=highlight>Multiword</span> <span class=highlight>Title</span>", List(MatchSpan(2, 9), MatchSpan(12, 5))))
    actualCompletions should contain theSameElementsAs expectedCompletions
  }

  test("an autocomplete search with mulitple copies of the same term works as expected") {
    val params = Map("q" -> "rammy").mapValues(Seq(_))
    val SearchResults(actualCompletions, _, _) = autocompleteService.doSearch(
      params, AuthParams(), None, None)._2
    val expectedCompletions = List(CompletionResult("rammy is rammy", "<span class=highlight>rammy</span> is <span class=highlight>rammy</span>", List(MatchSpan(0, 5), MatchSpan(9, 5))))
    actualCompletions should contain theSameElementsAs expectedCompletions
  }

  test("an autocomplete search restricted to a domain should return the expected results") {
    val basicDomain = domains(0).cname
    val params = Map("search_context" -> basicDomain, "domains" -> basicDomain, "q" -> "o")
      .mapValues(Seq(_))
    val SearchResults(actualCompletions, _, _) = autocompleteService.doSearch(
      params, AuthParams(), None, None)._2
    val expectedCompletions = List(CompletionResult("One", "<span class=highlight>O</span>ne", List(MatchSpan(0, 1))))
    actualCompletions should contain theSameElementsAs expectedCompletions
  }

  test("an autocomplete search honors the order parameter") {
    val params = Map("domains" -> domains.map(_.cname).mkString(","), "q" -> "t", "order" -> "name")
      .mapValues(Seq(_))

    val SearchResults(actualCompletions, _, _) = autocompleteService.doSearch(
      params, AuthParams(), None, None)._2

    val expectedCompletions = List("A Multiword Title", "Three", "Two")
    actualCompletions.map(_.title) should contain theSameElementsInOrderAs expectedCompletions
  }

  test("by default, autocomplete requires that all specified terms match") {
    val params = Map("domains" -> domains.map(_.cname).mkString(","), "q" -> "latest foo")
      .mapValues(Seq(_))

    val SearchResults(actualCompletions1, _, _) = autocompleteService.doSearch(
      params, AuthParams(), None, None)._2

    actualCompletions1.map(_.title) should contain theSameElementsInOrderAs List.empty[String]

    val SearchResults(actualCompletions2, _, _) = autocompleteService.doSearch(
      params + ("q" -> Seq("latest data")), AuthParams(), None, None)._2

    actualCompletions2.map(_.title) should contain theSameElementsInOrderAs List("My Latest and Greatest Dataset")
  }

  test("autocomplete honors the min_should_match parameter") {
    val params = Map("domains" -> domains.map(_.cname).mkString(","), "q" -> "latest foo")
      .mapValues(Seq(_))

    val SearchResults(actualCompletions1, _, _) = autocompleteService.doSearch(
      params, AuthParams(), None, None)._2

    actualCompletions1.map(_.title) should contain theSameElementsInOrderAs List.empty[String]

    val SearchResults(actualCompletions2, _, _) = autocompleteService.doSearch(
      params ++ Map("q" -> Seq("latest foo"), "min_should_match" -> Seq("1")), AuthParams(), None, None)._2

    actualCompletions2.map(_.title) should contain theSameElementsInOrderAs List("My Latest and Greatest Dataset")
  }
}
