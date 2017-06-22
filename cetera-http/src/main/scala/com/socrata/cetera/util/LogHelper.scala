package com.socrata.cetera.util

import scala.io.Source

import com.socrata.http.client.RequestBuilder
import com.socrata.http.server.HttpRequest
import org.elasticsearch.action.search.SearchRequestBuilder

import com.socrata.cetera._
import com.socrata.cetera.response.InternalTimings

object LogHelper {
  val PathSeparator = "/"
  val NewLine = "\n"

  // WARN: changing this will likely break Sumo (regex-based log parser)
  def formatRequest(request: HttpRequest, timings: InternalTimings): String = {
    List(
      "[" + request.servletRequest.getMethod + "]",
      request.requestPathStr,
      request.queryStr.getOrElse(""),
      "requested by",
      request.servletRequest.getRemoteHost,
      s"extended host = ${request.header(HeaderXSocrataHostKey)}",
      s"request id = ${request.header(HeaderXSocrataRequestIdKey)}",
      s"user agent = ${request.header(HeaderUserAgent)}",
      s"""TIMINGS ## ESTime : ${timings.searchMillis} ## ServiceTime : ${timings.serviceMillis}"""
    ).mkString(" -- ")
  }

  def formatEsRequest(search: SearchRequestBuilder): String = {
    s"""Elasticsearch request body: ${search.toString.replaceAll("""[\n\s]+""", " ")}
     """.stripMargin.trim
  }

  def formatHttpRequestVerbose(req: HttpRequest): String = {
    val sb = StringBuilder.newBuilder

    sb.append(s"received http request:\n")
    sb.append(s"${req.method} ${req.requestPath.mkString(PathSeparator, PathSeparator,"")}${req.queryStr.map("?" + _)
      .getOrElse("")}$NewLine")
    req.headerNames.foreach { n =>
      sb.append(s"$n: ${req.headers(n).mkString(", ")}$NewLine")
    }
    sb.append("\n")
    Source.fromInputStream(req.inputStream).getLines().foreach(line => sb.append(s"$line$NewLine"))

    sb.toString()
  }

  def formatSimpleHttpRequestBuilderVerbose(req: RequestBuilder): String = {
    val sb = StringBuilder.newBuilder

    sb.append(s"sending http request:$NewLine")
    sb.append(s"${req.method.getOrElse("GET")} ${req.path.mkString(PathSeparator, PathSeparator, "")}$NewLine")
    sb.append(req.headers.map { case (k,v) => s"$k: $v" }.mkString(NewLine))

    sb.toString()
  }
}
