package com.socrata.cetera.util

import scala.io.Source

import com.rojoma.json.v3.ast.JValue
import com.rojoma.json.v3.util.JsonUtil
import org.elasticsearch.ResourceAlreadyExistsException
import org.joda.time.DateTime
import org.slf4j.LoggerFactory

import com.socrata.cetera._
import com.socrata.cetera.errors.JsonDecodeException
import com.socrata.cetera.search.ElasticSearchClient

object ElasticsearchBootstrap {
  private def indexSettings: String = {
    val s = Source.fromInputStream(getClass.getResourceAsStream("/settings.json")).getLines().mkString("\n")
    val sj = JsonUtil.parseJson[JValue](s) match {
      case Left(e) => throw new JsonDecodeException(e)
      case Right(j) => j
    }
    sj.dyn.settings.!.toString()
  }

  private def jValueFromResource(resourcePath: String): JValue = {
    val s = Source.fromInputStream(getClass.getResourceAsStream(resourcePath)).getLines().mkString("\n")
    JsonUtil.parseJson[JValue](s) match {
      case Left(e) => throw new JsonDecodeException(e)
      case Right(j) => j
    }
  }

  private def datatypeMappings(datatype: String): String = {
    val typeMapping = datatype match {
      case s: String if s == "document" => jValueFromResource("/mapping_document.json")
      case s: String if s == "domain" => jValueFromResource("/mapping_domain.json")
      case s: String if s == "user" => jValueFromResource("/mapping_user.json")
    }

    typeMapping.toString()
  }

  def ensureIndex(client: ElasticSearchClient,
                  indexBootstrapDatetimeFormatPattern: String,
                  indexAliasName: String): Unit = {
    val logger = LoggerFactory.getLogger(getClass)
    if (!client.indexExists) {
      try {
        val index = DateTime.now.toString(indexBootstrapDatetimeFormatPattern) + "_" + indexAliasName

        logger.info(s"creating index $index")
        client.client.admin.indices.prepareCreate(index)
          .setSettings(indexSettings)
          .execute.actionGet

        client.client.admin.indices.preparePutMapping(index)
          .setType(esDocumentType)
          .setSource(datatypeMappings(esDocumentType))
          .execute.actionGet

        client.client.admin.indices.preparePutMapping(index)
          .setType(esDomainType)
          .setSource(datatypeMappings(esDomainType))
          .execute.actionGet

        client.client.admin.indices.preparePutMapping(index)
          .setType(esUserType)
          .setSource(datatypeMappings(esUserType))
          .execute.actionGet

        logger.info(s"aliasing $indexAliasName -> $index")
        client.client.admin.indices.prepareAliases
          .addAlias(index, indexAliasName)
          .execute.actionGet

      } catch {
        case e: ResourceAlreadyExistsException =>
          logger.info(s"actually that index ($indexAliasName) already exists!")
      }
    }
  }
}
