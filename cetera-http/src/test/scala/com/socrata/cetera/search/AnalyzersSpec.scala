package com.socrata.cetera.search

import org.scalatest.BeforeAndAfterAll
import scala.collection.JavaConverters._
import org.elasticsearch.action.admin.indices.analyze.AnalyzeRequest
import org.elasticsearch.action.admin.indices.analyze.AnalyzeResponse.AnalyzeToken
import org.scalatest.{ShouldMatchers, WordSpec}

import com.socrata.cetera.TestESData

case class Token(text: String, offset: Int)

object Token {
  def apply(token: AnalyzeToken): Token =
    Token(token.getTerm, token.getStartOffset)
}

class AnalyzersSpec extends WordSpec with ShouldMatchers with BeforeAndAfterAll with TestESData {
  override protected def beforeAll(): Unit = bootstrapData()

  def analyze(analyzer: String, text: String): List[Token] = {
    val request = new AnalyzeRequest(testSuiteName).analyzer(analyzer).text(text)
    client.client.admin().indices().analyze(request).actionGet().getTokens.asScala.map(Token.apply).toList
  }

  "the lowercase_alphanumeric analyzer" should {
    "do nothing to a string comprised of all lowercase letters" in {
      analyze("lowercase_alphanumeric", "lowercase") should be(List(Token("lowercase", 0)))
    }

    "lowercase all letters" in {
      analyze("lowercase_alphanumeric", "UpPeRaNdLoWeR") should be(List(Token("upperandlower", 0)))
    }

    "preserve accented characters" in {
      analyze("lowercase_alphanumeric", "éléphant") should be(List(Token("éléphant", 0)))
    }

    "replace one or more non-alphanumeric characters with a space" in {
      analyze("lowercase_alphanumeric", "foo-bar#baz") should be(List(Token("foo bar baz", 0)))
    }

    "preserve numbers" in {
      analyze("lowercase_alphanumeric", "123456789") should be(List(Token("123456789", 0)))
    }

    "trim initial and terminal non-alphanumeric characters" in {
      analyze("lowercase_alphanumeric", "  trim  me  PLEASE!!!") should be(List(Token("trim me please", 2)))
    }
  }
}
