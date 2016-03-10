package com.socrata.cetera.util

import com.socrata.http.server.HttpRequest
import org.elasticsearch.action.search.SearchRequestBuilder

object LogHelper {
  // WARN: changing this will likely break Sumo (regex-based log parser)
  def formatRequest(request: HttpRequest, timings: InternalTimings): String = {
    List[String](
      "[" + request.servletRequest.getMethod + "]",
      request.requestPathStr,
      request.queryStr.getOrElse(""),
      "requested by",
      request.servletRequest.getRemoteHost,
      s"""TIMINGS ## ESTime : ${timings.searchMillis} ## ServiceTime : ${timings.serviceMillis}"""
    ).mkString(" -- ")
  }

  def formatEsRequest(search: SearchRequestBuilder): String = {
    s"""Elasticsearch request body: ${search.toString.replaceAll("""[\n\s]+""", " ")}
     """.stripMargin.trim
  }
}
