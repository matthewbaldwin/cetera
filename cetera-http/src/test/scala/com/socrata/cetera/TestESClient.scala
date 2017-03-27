package com.socrata.cetera

import java.io.File
import java.nio.file.Files

import org.elasticsearch.common.settings.Settings
import org.elasticsearch.node.Node

import com.socrata.cetera.search.ElasticSearchClient

class TestESClient(testSuiteName: String)
  extends ElasticSearchClient("", 0, "", testSuiteName) { // host:port & cluster name are immaterial

  val tempDataDir = Files.createTempDirectory("elasticsearch_data_").toFile

  val testSettings = Settings.builder()
    .put("cluster.name", testSuiteName)
    .put("path.data", tempDataDir.toString)
    .put("path.home", "target/elasticsearch")
    .put("transport.type", "local")
    .put("http.enabled", "false")
    .build()

  val node = new Node(testSettings).start()

  override val client = node.client()

  override def close(): Unit = {
    node.close()

    try { // don't care if cleanup succeeded or failed
      def deleteR(file: File): Unit = {
        if (file.isDirectory) Option(file.listFiles).map(_.toList).getOrElse(Nil).foreach(deleteR)
        file.delete()
      }
      deleteR(tempDataDir)
    } catch { case e: Exception => () }

    super.close()
  }
}
