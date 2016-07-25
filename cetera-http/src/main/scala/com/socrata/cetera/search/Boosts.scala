package com.socrata.cetera.search

import org.elasticsearch.index.query.functionscore.{FunctionScoreQueryBuilder, ScoreFunctionBuilders}
import org.elasticsearch.index.query.{BoolQueryBuilder, FilterBuilders, QueryBuilders}
import com.socrata.cetera.types.{
  Datatype, Datatypes, DatatypeFieldType, ScriptScoreFunction, SocrataIdDomainIdFieldType}

object Boosts {
  def applyDatatypeBoosts(
      query: FunctionScoreQueryBuilder,
      datatypeBoosts: Map[Datatype, Float])
    : Unit = {

    datatypeBoosts.foreach {
      case (datatype, boost) =>
        query.add(
          FilterBuilders.termFilter(DatatypeFieldType.fieldName, datatype.singular),
          ScoreFunctionBuilders.weightFactorFunction(boost)
        )
    }
  }

  def applyScoreFunctions(
      query: FunctionScoreQueryBuilder,
      scriptScoreFunctions: Set[ScriptScoreFunction])
    : Unit = {

    scriptScoreFunctions.foreach { fn =>
      query.add(ScoreFunctionBuilders.scriptFunction(fn.script, "expression"))
    }
  }

  def applyDomainBoosts(
      query: FunctionScoreQueryBuilder,
      domainIdBoosts: Map[Int, Float])
    : Unit = {

    domainIdBoosts.foreach {
      case (domainId, weight) =>
        query.add(
          FilterBuilders.termFilter(SocrataIdDomainIdFieldType.fieldName, domainId),
          ScoreFunctionBuilders.weightFactorFunction(weight)
        )
    }
  }
}
