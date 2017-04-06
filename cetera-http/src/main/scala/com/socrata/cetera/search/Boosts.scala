package com.socrata.cetera.search

import scala.collection.JavaConverters._

import org.elasticsearch.script.{Script, ScriptType}
import org.elasticsearch.index.query.functionscore._
import org.elasticsearch.index.query.functionscore.FunctionScoreQueryBuilder.FilterFunctionBuilder
import org.elasticsearch.index.query.QueryBuilders.termQuery

import com.socrata.cetera.types._
import com.socrata.cetera.handlers.AgeDecayParamSet

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

  val officialProvenance = "official"

  def officialBoostFunction(weight: Float): Option[FilterFunctionBuilder] =
    weight match {
      // It's not worth applying this filter in the case that it does nothing
      case 1.0f => None
      case _ => Some(
        new FilterFunctionBuilder(
          termQuery(ProvenanceFieldType.fieldName, officialProvenance),
          ScoreFunctionBuilders.weightFactorFunction(weight)))
    }

  val ExponentialDecay = "exp"
  val GaussianDecay = "gauss"
  val LinearDecay = "linear"
  val ValidDecayTypes = Set(ExponentialDecay, GaussianDecay, LinearDecay)

  def isValidDecayType(decayType: String): Boolean =
    ValidDecayTypes.contains(decayType)

  def ageDecayFunction(ageDecayParams: AgeDecayParamSet): FilterFunctionBuilder = {
    val AgeDecayParamSet(decayType, scale, decay, offset, origin) = ageDecayParams

    decayType match {
      case ExponentialDecay =>
        new FilterFunctionBuilder(
          new ExponentialDecayFunctionBuilder(
            UpdatedAtFieldType.fieldName, origin, scale, offset, decay))
      case GaussianDecay =>
        new FilterFunctionBuilder(
          new GaussDecayFunctionBuilder(
            UpdatedAtFieldType.fieldName, origin, scale, offset, decay))
      case LinearDecay =>
        new FilterFunctionBuilder(
          new LinearDecayFunctionBuilder(
            UpdatedAtFieldType.fieldName, origin, scale, offset, decay))
    }
  }
}

class ScriptScoreFunction(val script: String)

object ScriptScoreFunction {
  def apply(name: String): Option[ScriptScoreFunction] =
    name match {
      case "views" => Option(new ScriptScoreFunction("""1 + doc["page_views.page_views_total_log"].value"""))
      case "score" => Option(new ScriptScoreFunction("_score"))
      case _ => None // log this?
    }
}
