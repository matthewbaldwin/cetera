package com.socrata.cetera.search

import scala.collection.JavaConverters._

import org.elasticsearch.script.{Script, ScriptType}
import org.elasticsearch.index.query.functionscore._
import FunctionScoreQueryBuilder.FilterFunctionBuilder
import org.elasticsearch.index.query.QueryBuilders.termQuery
import com.socrata.cetera.types.{
  Datatype, DatatypeFieldType, ScriptScoreFunction, SocrataIdDomainIdFieldType}

object Boosts {
  def datatypeBoostFunctions(
      datatypeBoosts: Map[Datatype, Float])
    : List[FilterFunctionBuilder] =
    datatypeBoosts.map {
      case (datatype, boost) =>
        new FilterFunctionBuilder(
          termQuery(DatatypeFieldType.fieldName, datatype.singular),
          ScoreFunctionBuilders.weightFactorFunction(boost)
        )
    }.toList

  def scriptScoreFunctions(
      scriptScoreFunctions: Set[ScriptScoreFunction])
    : List[FilterFunctionBuilder] =
    scriptScoreFunctions.map { fn =>
      val script = new Script(
        ScriptType.INLINE, "expression", fn.script, Map.empty[String, Object].asJava)
      new FilterFunctionBuilder(ScoreFunctionBuilders.scriptFunction(script))
    }.toList

  def domainBoostFunctions(
      domainIdBoosts: Map[Int, Float])
    : List[FilterFunctionBuilder] =
    domainIdBoosts.map {
      case (domainId, weight) =>
        new FilterFunctionBuilder(
          termQuery(SocrataIdDomainIdFieldType.fieldName, domainId),
          ScoreFunctionBuilders.weightFactorFunction(weight)
        )
    }.toList
}
