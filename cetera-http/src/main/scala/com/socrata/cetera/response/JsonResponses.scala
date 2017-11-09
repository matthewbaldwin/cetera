package com.socrata.cetera.response

import java.io.{PrintWriter, StringWriter}
import scala.annotation.tailrec
import scala.collection.JavaConverters._

import com.rojoma.json.v3.codec.JsonEncode
import com.rojoma.json.v3.util._
import com.socrata.http.server.HttpResponse
import com.socrata.http.server.responses.{Json, StatusResponse, Unauthorized}
import org.elasticsearch.common.text.Text
import org.elasticsearch.search.SearchHit

import com.socrata.cetera.types._

object JsonResponses {
  def jsonError(error: String): HttpResponse = {
    val errorMap = Map("error" -> error)
    Json(errorMap)
  }

  def jsonError(context: String, error: Throwable): HttpResponse = {
    Json(Map("context" -> context,
      "error" -> error.toString,
      "stackTrace" -> getStackTraceAsString(error)
    ))}

  // from http://alvinalexander.com/scala/how-convert-stack-trace-exception-string-print-logger-logging-log4j-slf4j
  private def getStackTraceAsString(t: Throwable) = {
    val sw = new StringWriter
    t.printStackTrace(new PrintWriter(sw))
    sw.toString
  }
}

// Helpers for internal timing information to report to the caller
case class InternalTimings(serviceMillis: Long, searchMillis: Seq[Long])

object InternalTimings {
  implicit val jCodec = AutomaticJsonCodecBuilder[InternalTimings]
}

@JsonKeyStrategy(Strategy.Underscore)
case class Classification(
    categories: Seq[String],
    tags: Seq[String],
    domainCategory: Option[String],
    domainTags: Seq[String],
    domainMetadata: Seq[CustomerMetadataFlattened],
    domainPrivateMetadata: Option[Seq[CustomerMetadataFlattened]])

object Classification {
  implicit val jCodec = AutomaticJsonCodecBuilder[Classification]
}

@JsonKeyStrategy(Strategy.Underscore)
case class Metadata(
    domain: String,
    license: Option[String] = None,
    isPublic: Option[Boolean] = None,
    isPublished: Option[Boolean] = None,
    isHidden: Option[Boolean] = None,
    isModerationApproved: Option[Boolean] = None,
    isModerationApprovedOnContext: Option[Boolean] = None,
    isRoutingApproved: Option[Boolean] = None,
    isRoutingApprovedOnContext: Option[Boolean] = None,
    visibleToAnonymous: Option[Boolean] = None,
    moderationStatus: Option[String] = None,
    routingStatus: Option[String] = None,
    datalensStatus: Option[String] = None,
    score: Option[Float] = None,
    grants: Option[Seq[ESGrant]] = None,
    approvals: Option[Seq[FlattenedApproval]] = None)

object Metadata {
  implicit val jCodec = AutomaticJsonCodecBuilder[Metadata]
}

@JsonKeyStrategy(Strategy.Underscore)
case class SearchResult(
    resource: Resource,
    classification: Classification,
    metadata: Metadata,
    permalink: String,
    link: String,
    previewImageUrl: Option[String],
    owner: UserInfo)

object SearchResult {
  implicit val jCodec = AutomaticJsonCodecBuilder[SearchResult]
}

case class SearchResults[T](
    results: Seq[T],
    resultSetSize: Long,
    timings: Option[InternalTimings] = None)

object SearchResults {
  implicit def jEncode[T: JsonEncode]: JsonEncode[SearchResults[T]] = {
    AutomaticJsonEncodeBuilder[SearchResults[T]]
  }

  def returnUnauthorized[T](setCookies: Seq[String], time: Long)
  : (StatusResponse, SearchResults[T], InternalTimings, Seq[String]) =
    (
      Unauthorized,
      SearchResults(Seq.empty[T], 0),
      InternalTimings(Timings.elapsedInMillis(time), Seq(0)),
      setCookies
    )
}

case class MatchSpan(start: Int, length: Int)

object MatchSpan {
  implicit val jCodec = AutomaticJsonCodecBuilder[MatchSpan]

  val HighlightPattern = """<span class=highlight>(.*?)</span>""".r

  def fromHighlightedString(highlighted: String): List[MatchSpan] = {
    @tailrec
    def inner(text: CharSequence, matches: List[MatchSpan] = Nil): List[MatchSpan] =
      HighlightPattern.findFirstMatchIn(text) match {
        case None => matches
        case Some(m) =>
          val start = m.start(0)
          val length = m.group(1).length
          val newText = m.before(0) + m.group(1) + m.after(0)
          inner(newText, MatchSpan(start, length) :: matches)
      }

    inner(highlighted).reverse
  }
}

@JsonKeyStrategy(Strategy.Underscore)
case class CompletionResult(title: String, displayTitle: String, matchOffsets: Seq[MatchSpan])

object CompletionResult {
  implicit val jCodec = AutomaticJsonCodecBuilder[CompletionResult]

  def fromElasticsearchHit(hit: SearchHit): CompletionResult = {
    val title = TitleFieldType.fromSearchHit(hit)
    val highlightMap = hit.getHighlightFields.asScala
    val highlightField = highlightMap.get(TitleFieldType.autocompleteFieldName)

    val displayTitle = highlightField.flatMap(field =>
      field.fragments.collect { case t: Text => t.toString }.headOption
    ).getOrElse(title)

    val spans = MatchSpan.fromHighlightedString(displayTitle)

    CompletionResult(title, displayTitle, spans)
  }
}

@JsonKeyStrategy(Strategy.Underscore)
case class CompletionMatch(field: String, offsets: Seq[MatchSpan])

object CompletionMatch {
  implicit val jCodec = AutomaticJsonCodecBuilder[CompletionMatch]
}

@JsonKeyStrategy(Strategy.Underscore)
case class UserCompletionResult(matches: Seq[CompletionMatch], user: DomainUser)

object UserCompletionResult {
  implicit val jCodec = AutomaticJsonCodecBuilder[UserCompletionResult]
}
