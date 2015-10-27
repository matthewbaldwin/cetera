package com.socrata.cetera.types

object Datatypes {
  val all: Seq[Materialized] = Seq(TypeCalendars, TypeCharts, TypeDatalensCharts, TypeDatalenses, TypeDatalensMaps,
    TypeDatasets, TypeFiles, TypeFilters, TypeForms, TypeGeoMaps, TypeHrefs, TypePulses, TypeTabularMaps)
}

abstract class DatatypeSimple {
  def names: Seq[String] = Seq(plural)
  val plural: String
  def singular: String = plural.dropRight(1)
}
object DatatypeSimple {
  def apply(s: String): Option[DatatypeSimple] = {
    Datatypes.all.find(d => d.plural == s || d.singular == s).headOption
  }
}

trait DatatypeRename extends DatatypeSimple{
  override def names: Seq[String]
}

trait Materialized extends DatatypeSimple

case object TypeCalendars extends DatatypeSimple with Materialized {
  val plural = "calendars"
}
case object TypeDatalenses extends DatatypeSimple with Materialized {
  val plural: String = "datalenses"
  override val singular: String = "datalens"
}
case object TypeDatasets extends DatatypeSimple with Materialized {
  val plural: String = "datasets"
}
case object TypeFiles extends DatatypeSimple with Materialized {
  val plural: String = "files"
}
case object TypeFilters extends DatatypeSimple with Materialized {
  val plural: String = "filters"
}
case object TypeForms extends DatatypeSimple with Materialized {
  val plural: String = "forms"
}
case object TypeHrefs extends DatatypeSimple with Materialized {
  val plural: String = "hrefs"
}
case object TypePulses extends DatatypeSimple with Materialized {
  val plural: String = "pulses"
}

// charts
case object TypeCharts extends DatatypeRename with Materialized {
  val plural = "charts"
  override def names: Seq[String] = Seq(TypeCharts.plural, TypeDatalensCharts.plural)
}
case object TypeDatalensCharts extends DatatypeRename with Materialized {
  val plural: String = "datalens_charts"
  override def names: Seq[String] = Seq(TypeCharts.plural, TypeDatalensCharts.plural)
}

// maps
case object TypeMaps extends DatatypeRename {
  val plural: String = "maps"
  override def names: Seq[String] = Seq(TypeDatalensMaps.plural, TypeGeoMaps.plural, TypeTabularMaps.plural)
}
case object TypeDatalensMaps extends DatatypeRename with Materialized {
  val plural: String = "datalens_maps"
  override def names: Seq[String] = Seq(TypeDatalensMaps.plural, TypeGeoMaps.plural, TypeTabularMaps.plural)
}
case object TypeGeoMaps extends DatatypeRename with Materialized {
  val plural: String = "geo_maps"
  override def names: Seq[String] = Seq(TypeDatalensMaps.plural, TypeGeoMaps.plural, TypeTabularMaps.plural)
}
case object TypeTabularMaps extends DatatypeRename with Materialized {
  val plural: String = "tabular_maps"
  override def names: Seq[String] = Seq(TypeDatalensMaps.plural, TypeGeoMaps.plural, TypeTabularMaps.plural)
}
