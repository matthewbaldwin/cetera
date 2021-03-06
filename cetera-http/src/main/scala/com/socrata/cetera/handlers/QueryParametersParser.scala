package com.socrata.cetera.handlers

import org.joda.time.{DateTime, DateTimeZone}
import org.slf4j.LoggerFactory

import com.socrata.cetera.handlers.util._
import com.socrata.cetera.search.{Boosts, NestedSort, SimpleSort, Sorts}
import com.socrata.cetera.types._

// These are validated input parameters but aren't supposed to know anything about ES
case class ValidatedQueryParameters(
    searchParamSet: SearchParamSet,
    scoringParamSet: ScoringParamSet,
    pagingParamSet: PagingParamSet,
    formatParamSet: FormatParamSet)

case class ValidatedUserQueryParameters(
    searchParamSet: UserSearchParamSet,
    scoringParamSet: UserScoringParamSet,
    pagingParamSet: PagingParamSet)

sealed trait ValidationError { def message: String }

case class DatatypeError(override val message: String) extends ValidationError

case class UserTypeError(override val message: String) extends ValidationError

// Parses and validates
object QueryParametersParser { // scalastyle:ignore number.of.methods
  val logger = LoggerFactory.getLogger(getClass)

  val filterDelimiter = "," // to be deprecated
  val limitLimit = 10000
  val maxResultWindow = 10000
  val allowedFilterTypesMsg = Datatypes.all.flatMap(d => Seq(d.plural, d.singular)).mkString(", ")
  val allowedUserTypesMsg = UserType.all.mkString(", ")

  def validated[T](x: Either[ParamConversionFailure, T]): T = x match {
    case Right(v) => v
    case Left(_) => throw new Exception("Parameter validation failure")
  }

  // for offset and limit
  case class NonNegativeInt(value: Int)
  implicit val nonNegativeIntParamConverter = ParamConverter.filtered { (a: Int) =>
    if (a >= 0) Some(NonNegativeInt(a)) else None
  }

  // for boosts
  case class NonNegativeFloat(value: Float)
  implicit val nonNegativeFloatParamConverter = ParamConverter.filtered { (a: Float) =>
    if (a >= 0.0f) Some(NonNegativeFloat(a)) else None
  }

  // for age decay
  implicit val ageDecayParamConverter = ParamConverter.filtered { (paramString: String) =>
    def now() = DateTime.now(DateTimeZone.UTC).toString

    val parts = {
      val paramsSplit = paramString.split(',')
      paramsSplit.length match {
        case 4 => Some(paramsSplit :+ now()) // scalastyle:ignore magic.number
        case 5 => Some(paramsSplit) // scalastyle:ignore magic.number
        case n: Int =>
          logger.error(s"Expected 4 or 5 comma-separated parameters to age_decay, got $n")
          None
      }
    }

    try {
      parts.collect {
        case Array(decayType, scale, decay, offset, origin) if (Boosts.isValidDecayType(decayType)) =>
          AgeDecayParamSet(decayType, scale, decay.toDouble, offset, DateTime.parse(origin))
      }
    } catch {
      case _: NumberFormatException =>
        logger.error(s"Invalid value for decay parameter: ${parts.get(2)}")
        None
      case _: IllegalArgumentException =>
        logger.error(s"Invalid date format for origin parameter: ${parts.last}")
        None
    }
  }

  // If both are specified, prefer the advanced query over the search query
  private def pickQuery(advanced: Option[String], simple: Option[String]): QueryType =
    (advanced, simple) match {
      case (Some(aq), _) => AdvancedQuery(aq)
      case (_, Some(sq)) => SimpleQuery(sq)
      case _ => NoQuery
    }

  private def mergeOptionalSets[T](ss: Set[Option[Set[T]]]): Option[Set[T]] = {
    val cs = ss.flatMap(_.getOrElse(Set.empty[T]))
    if (cs.nonEmpty) Some(cs) else None
  }

  // Used to support param=this,that and param[]=something&param[]=more
  // with the understanding that this produces values "this,that", "something" and "more"
  private def mergeArrayCommaParams(
      queryParameters: MultiQueryParams,
      key: String)
    : Option[Set[String]] = {
    val keys = Set(key, Params.arrayify(key))
    mergeOptionalSets(keys.map(key => queryParameters.get(key).map(_.toSet)))
  }


  // This can stay case-sensitive because it is so specific
  def restrictParamFilterDatatype(datatype: String): Either[DatatypeError, Option[Set[String]]] = datatype match {
    case s: String if s.nonEmpty =>
      Datatype(s) match {
        case None => Left(DatatypeError(s"'${Params.only}' must be one of $allowedFilterTypesMsg; got $s"))
        case Some(d) => Right(Some(d.names.toSet))
      }
    case _ => Right(None)
  }

  private def restrictApprovalFilter(status: String): ApprovalStatus =
    ApprovalStatus.all.find(_.status == status) match {
      case Some(s) => s
      case None => throw new IllegalArgumentException(
        s"'${Params.approvalStatus}' must be one of ${ApprovalStatus.all.map(_.status)}; got $status")
    }

  private def restrictVisibilityFilter(vis: String, queryParameters: MultiQueryParams): String = {
    if (!VisibilityStatus.all.contains(vis.toLowerCase)) {
      throw new IllegalArgumentException(s"'${Params.visibility}' must be one ${VisibilityStatus.all}; got $vis")
    }
    vis.toLowerCase
  }

  // for extracting `example.com` from `boostDomains[example.com]`
  class FieldExtractor(val key: String) {
    def unapply(s: String): Option[String] =
      if (s.startsWith(key + "[") && s.endsWith("]")) Some(s.drop(key.length + 1).dropRight(1)) else None
  }

  object FloatExtractor {
    def unapply(s: String): Option[Float] = try { Some(s.toFloat) } catch { case _: NumberFormatException => None }
  }

  private def filterNonEmptySetParams(params: Option[Set[String]]): Option[Set[String]] =
    params.flatMap { ps: Set[String] =>
      ps.filter(_.nonEmpty) match {
        case filteredParams: Set[String] if filteredParams.nonEmpty => Some(filteredParams)
        case _ => None
      }
    }

  private def filterNonEmptyStringParams(params: Option[String]): Option[String] =
    params.getOrElse("") match {
      case x: String if x.nonEmpty => params
      case _ => None
    }

  private def prepareBooleanParam(queryParameters: MultiQueryParams, param: String): Option[Boolean] = {
    if (queryParameters.contains(param)){
      queryParameters.first(param) match {
        case Some(b) => if (b.isEmpty) Some(true) else Some(b.toBoolean)
        case None => Some(true) // key was present without value
      }
    } else {
      None
    }
  }

  private def restrictParamFilterUserType(userType: String): Either[UserTypeError, Option[UserType]] =
    userType match {
      case s: String if s.nonEmpty =>
        UserType(s) match {
          case None => Left(UserTypeError(s"'${Params.only}' must be one of $allowedUserTypesMsg; got $s"))
          case Some(t) => Right(Some(t))
        }
      case _ => Right(None)
    }

  private def splitSortParam(sortParam: String): (String, Option[String]) = {
    val sortParts = sortParam.split(' ')
    val sortKey = sortParts(0)
    if (sortParts.size > 1) {
      val order = sortParts(1)
      if (order == Sorts.Ascending || order == Sorts.Descending) {
        (sortKey, Some(order))
      } else {
        throw new IllegalArgumentException(s"'$sortParam' is not a supported sort")
      }
    } else {
      (sortKey, None)
    }
  }

  //////////////////
  // PARAM PREPARERS
  //
  // Prepare means parse and validate

  def prepareSort(queryParameters: MultiQueryParams): (Option[String], Option[String]) = {
    val sortParam = queryParameters.first(Params.order)
    sortParam match {
      case None => (None, None)
      case Some(s) =>
        val (sortKey, sortOrder) = splitSortParam(s)
        if (sortKey == "relevance" ||
          sortKey == "dataset_id" ||
          SimpleSort(sortKey).isDefined ||
          NestedSort(sortKey).isDefined) {
          (Some(sortKey), sortOrder)
        } else {
          throw new IllegalArgumentException(s"'$s' is not a supported sort")
        }
    }
  }

  def prepareLocale(queryParametesrs: MultiQueryParams): Option[String] =
    queryParametesrs.first(Params.locale).map(_.toLowerCase)

  def prepareSearchQuery(queryParameters: MultiQueryParams): QueryType =
    pickQuery(
      filterNonEmptyStringParams(queryParameters.get(Params.qInternal).flatMap(_.headOption)),
      filterNonEmptyStringParams(queryParameters.get(Params.q).flatMap(_.headOption))
    )

  // Still uses old-style comma-separation
  def prepareDomains(queryParameters: MultiQueryParams): Option[Set[String]] =
    filterNonEmptySetParams(queryParameters.first(Params.domains).map(domain =>
      domain.toLowerCase.split(filterDelimiter).toSet
    ))

  def prepareSearchContext(queryParameters: MultiQueryParams): Option[String] = {
    val contextSet = queryParameters.first(Params.searchContext).map(_.toLowerCase)
    filterNonEmptyStringParams(contextSet)
  }

  def prepareCategories(queryParameters: MultiQueryParams): Option[Set[String]] =
    filterNonEmptySetParams(mergeArrayCommaParams(queryParameters, Params.categories))

  def prepareTags(queryParameters: MultiQueryParams): Option[Set[String]] =
    filterNonEmptySetParams(mergeArrayCommaParams(queryParameters, Params.tags))

  def prepareDatatypes(queryParameters: MultiQueryParams): Option[Set[String]] = {
    val csvParams = queryParameters.get(Params.only).map(_.flatMap(_.split(filterDelimiter))).map(_.toSet)
    val arrayParams = queryParameters.get(Params.arrayify(Params.only)).map(_.toSet)
    val mergedParams = mergeOptionalSets[String](Set(csvParams, arrayParams))

    mergedParams.flatMap { params =>
      val parsedDatatypes = params.map(restrictParamFilterDatatype)

      val errors = parsedDatatypes.collect { case Left(err) => err }.headOption
      val datatypes = parsedDatatypes.collect { case Right(Some(ds)) => ds }.flatten

      (errors, datatypes) match {
        case (Some(es), _) => throw new IllegalArgumentException(s"Invalid 'only' parameter: ${es.message}")
        case (_, ds)       => Some(ds)
        case _             => None
      }
    }
  }

  def prepareUsers(queryParameters: MultiQueryParams): Option[String] =
    filterNonEmptyStringParams(queryParameters.first(Params.forUser))

  def prepareSharedTo(queryParameters: MultiQueryParams): Option[String] =
    filterNonEmptyStringParams(queryParameters.first(Params.sharedTo))

  def prepareAttribution(queryParameters: MultiQueryParams): Option[String] =
    filterNonEmptyStringParams(queryParameters.first(Params.attribution))

  def prepareProvenance(queryParameters: MultiQueryParams): Option[String] =
    filterNonEmptyStringParams(queryParameters.first(Params.provenance))

  def prepareParentDatasetId(queryParameters: MultiQueryParams): Option[String] =
    filterNonEmptyStringParams(queryParameters.first(Params.derivedFrom))

  def preparePublic(queryParameters: MultiQueryParams): Option[Boolean] =
    prepareBooleanParam(queryParameters, Params.public)

  def preparePublished(queryParameters: MultiQueryParams): Option[Boolean] =
    prepareBooleanParam(queryParameters, Params.published)

  def prepareDerived(queryParameters: MultiQueryParams): Option[Boolean] =
    prepareBooleanParam(queryParameters, Params.derived)

  def prepareHidden(queryParameters: MultiQueryParams): Option[Boolean] =
    prepareBooleanParam(queryParameters, Params.explicitlyHidden)

  def prepareLicense(queryParameters: MultiQueryParams): Option[String] =
    filterNonEmptyStringParams(queryParameters.first(Params.license))

  def prepareColumnNames(queryParameters: MultiQueryParams): Option[Set[String]] =
    filterNonEmptySetParams(mergeArrayCommaParams(queryParameters, Params.columnNames))

  def prepareSubmitterId(queryParameters: MultiQueryParams): Option[String] =
    filterNonEmptyStringParams(queryParameters.first(Params.submitterId))

  def prepareReviewerId(queryParameters: MultiQueryParams): Option[String] =
    filterNonEmptyStringParams(queryParameters.first(Params.reviewerId))

  def prepareReviewedAutomatically(queryParameters: MultiQueryParams): Option[Boolean] =
    prepareBooleanParam(queryParameters, Params.reviewedAutomatically)

  def prepareApprovalStatus(queryParameters: MultiQueryParams): Option[ApprovalStatus] = {
    val status = filterNonEmptyStringParams(queryParameters.first(Params.approvalStatus))
    status.map(restrictApprovalFilter(_))
  }

  def prepareVisibility(queryParameters: MultiQueryParams): Option[String] = {
    val vis = filterNonEmptyStringParams(queryParameters.first(Params.visibility))
    vis.map(restrictVisibilityFilter(_, queryParameters))
  }

  def prepareDomainMetadata(queryParameters: MultiQueryParams): Option[Set[(String, String)]] = {
    val queryParamsNonEmpty = queryParameters.filter { case (key, value) => key.nonEmpty && value.nonEmpty }
    // TODO: EN-1401 - don't just take head, allow for mulitple selections
    val ms = Params.remaining(queryParamsNonEmpty).mapValues(_.head).toSet
    if (ms.nonEmpty) Some(ms) else None
  }

  def prepareFieldBoosts(queryParameters: MultiQueryParams): Map[CeteraFieldType with Boostable, Float] = {
    val boostTitle = queryParameters.typedFirst[NonNegativeFloat](Params.boostTitle).map(validated(_).value)
    val boostDesc = queryParameters.typedFirst[NonNegativeFloat](Params.boostDescription).map(validated(_).value)
    val boostColumns = queryParameters.typedFirst[NonNegativeFloat](Params.boostColumns).map(validated(_).value)

    val fieldBoosts = Map(
      TitleFieldType -> boostTitle,
      DescriptionFieldType -> boostDesc,

      ColumnNameFieldType -> boostColumns,
      ColumnDescriptionFieldType -> boostColumns,
      ColumnFieldNameFieldType -> boostColumns
    )

    fieldBoosts
      .collect { case (fieldType, Some(weight)) => (fieldType, weight) }
      .toMap[CeteraFieldType with Boostable, Float]
  }

  def prepareDatatypeBoosts(queryParameters: MultiQueryParams): Map[Datatype, Float] = {
    queryParameters.flatMap {
      case (k, _) => Params.datatypeBoostParam(k).flatMap(datatype =>
        queryParameters.typedFirst[NonNegativeFloat](k).map(boost =>
          (datatype, validated(boost).value)))
    }
  }

  def prepareDomainBoosts(queryParameters: MultiQueryParams): Map[String, Float] = {
    val domainExtractor = new FieldExtractor(Params.boostDomains)
    queryParameters.collect { case (domainExtractor(field), Seq(FloatExtractor(weight))) =>
      field -> weight
    }
  }

  def prepareOfficialBoost(queryParameters: MultiQueryParams): Float =
    queryParameters.typedFirst[NonNegativeFloat](Params.boostOfficial).map(validated(_).value).getOrElse(1.0f)

  // Yes we compute searchQuery twice because this is a smell
  def prepareMinShouldMatch(queryParameters: MultiQueryParams): Option[String] = {
    val searchQuery = prepareSearchQuery(queryParameters)
    queryParameters.first(Params.minShouldMatch).flatMap { p =>
      MinShouldMatch.fromParam(searchQuery, p)
    }
  }

  def prepareSlop(queryParameters: MultiQueryParams): Option[Int] =
    queryParameters.typedFirst[Int](Params.slop).map(validated)

  // EXAMPLE: age_decay=gauss,365d,0.5,14d
  // EXAMPLE: age_decay=gauss,182d,0.5,14d,2017-04-05
  def prepareAgeDecay(queryParameters: MultiQueryParams): Option[AgeDecayParamSet] =
    queryParameters.typedFirst[AgeDecayParamSet](Params.ageDecay).map(validated)

  def prepareShowScore(queryParameters: MultiQueryParams): Boolean =
    prepareBooleanParam(queryParameters, Params.showScore).getOrElse(false)

  def prepareShowVisiblity(queryParameters: MultiQueryParams): Boolean =
    prepareBooleanParam(queryParameters, Params.showVisibility).getOrElse(false)

  def prepareOffset(queryParameters: MultiQueryParams): Int =
    validated(
      queryParameters.typedFirstOrElse(Params.offset, NonNegativeInt(PagingParamSet.defaultPageOffset))
    ).value

  def prepareScrollId(queryParameters: MultiQueryParams): Option[String] =
    queryParameters.get(Params.scrollId).flatMap(_.headOption)

  def prepareLimit(queryParameters: MultiQueryParams): Int =
    Math.min(
      limitLimit,
      validated(
        queryParameters.typedFirstOrElse(Params.limit, NonNegativeInt(PagingParamSet.defaultPageLength))
      ).value
    )

  //  user search param preparers

  def prepareEmail(queryParameters: MultiQueryParams): Option[Set[String]] =
    filterNonEmptySetParams(mergeArrayCommaParams(queryParameters, Params.emails))

  def prepareScreenName(queryParameters: MultiQueryParams): Option[Set[String]] =
    filterNonEmptySetParams(mergeArrayCommaParams(queryParameters, Params.screenNames))

  def prepareFlag(queryParameters: MultiQueryParams): Option[Set[String]] =
    filterNonEmptySetParams(mergeArrayCommaParams(queryParameters, Params.flags))

  def prepareRoleName(queryParameters: MultiQueryParams): Option[Set[String]] =
    filterNonEmptySetParams(mergeArrayCommaParams(queryParameters, Params.roles))

  def prepareRoleId(queryParameters: MultiQueryParams): Option[Set[Int]] =
    mergeArrayCommaParams(queryParameters, Params.roleIds).map(_.map(roleId => NonNegativeInt(roleId.toInt).value))

  def prepareUserDomain(queryParameters: MultiQueryParams): Option[String] =
    filterNonEmptyStringParams(queryParameters.first(Params.domain))

  def prepareUserQuery(queryParameters: MultiQueryParams): Option[String] =
    filterNonEmptyStringParams(queryParameters.first(Params.q))

  def prepareUserType(queryParameters: MultiQueryParams): Option[UserType] =
    filterNonEmptyStringParams(queryParameters.first(Params.only)).map(restrictParamFilterUserType).collect {
      case Left(err) => throw new IllegalArgumentException(s"Invalid 'only' parameter: ${err.message}")
      case Right(Some(userType)) => userType
    }

  // shared param preparers

  def prepareId(queryParameters: MultiQueryParams): Option[Set[String]] =
    filterNonEmptySetParams(mergeArrayCommaParams(queryParameters, Params.ids))

  // param set validators
  def validatePagingParams(pagingParams: PagingParamSet): Unit = {
    if (pagingParams.offset + pagingParams.limit > maxResultWindow) {
      throw new IllegalArgumentException(s"Sum of `offset` and `limit` cannot exceed $maxResultWindow")
    }

    if (pagingParams.scrollId.isDefined && pagingParams.sortKey.isDefined) {
      throw new IllegalArgumentException(s"The `order` and `scroll_id` parameters cannot both be specified")
    }

    if (pagingParams.scrollId.isDefined && pagingParams.offset > 0) {
      throw new IllegalArgumentException(s"The `offset` and `scroll_id` parameters cannot both be specified")
    }
  }

  def validateSearchParams(searchParams: SearchParamSet): Unit = {
    def invalidParameterCombination(visibilityStatus: String, param: String, value: String): String =
      s"Invalid parameter combination: ${Params.visibility}=$visibilityStatus and ${param}={$value}"

    searchParams.visibility match {
      case Some(visibility) if (visibility == VisibilityStatus.open) =>
        if (searchParams.public.exists(_ == false)) {  // scalastyle:ignore simplify.boolean.expression
          throw new IllegalArgumentException(
            invalidParameterCombination(VisibilityStatus.open, Params.public, "false"))
        }

        if (searchParams.published.exists(_ == false)) {  // scalastyle:ignore simplify.boolean.expression
          throw new IllegalArgumentException(
            invalidParameterCombination(VisibilityStatus.open, Params.published, "false"))
        }

        if (searchParams.explicitlyHidden.exists(_ == true)) {  // scalastyle:ignore simplify.boolean.expression
          throw new IllegalArgumentException(
            invalidParameterCombination(VisibilityStatus.open, Params.explicitlyHidden, "true"))
        }

        if (searchParams.approvalStatus.exists(_ != ApprovalStatus.approved)) {
          throw new IllegalArgumentException(
            invalidParameterCombination(
              VisibilityStatus.open, Params.approvalStatus,
              searchParams.approvalStatus.map(_.status).getOrElse("(pending|rejected)")))
        }
      // NOTE: all combinations of visibility=internal with other params is legitimate, except when
      // all four vis params define visibility=open
      case _ =>
    }
  }


  //////////////////////
  // ParamSet builders

  def searchParamSet(queryParameters: MultiQueryParams): SearchParamSet = {
    val searchParams = SearchParamSet(
      prepareSearchQuery(queryParameters),
      prepareDomains(queryParameters),
      prepareSearchContext(queryParameters),
      prepareDomainMetadata(queryParameters),
      prepareCategories(queryParameters),
      prepareTags(queryParameters),
      prepareDatatypes(queryParameters),
      prepareUsers(queryParameters),
      prepareSharedTo(queryParameters),
      prepareAttribution(queryParameters),
      prepareProvenance(queryParameters),
      prepareParentDatasetId(queryParameters),
      prepareId(queryParameters),
      preparePublic(queryParameters),
      preparePublished(queryParameters),
      prepareDerived(queryParameters),
      prepareHidden(queryParameters),
      prepareApprovalStatus(queryParameters),
      prepareVisibility(queryParameters),
      prepareLicense(queryParameters),
      prepareColumnNames(queryParameters),
      prepareSubmitterId(queryParameters),
      prepareReviewerId(queryParameters),
      prepareReviewedAutomatically(queryParameters)
    )

    validateSearchParams(searchParams)
    searchParams
  }

  def pagingParamSet(queryParameters: MultiQueryParams): PagingParamSet = {
    val (sortKey, sortOrder) = prepareSort(queryParameters)
    val pagingParams = PagingParamSet(
      prepareOffset(queryParameters),
      prepareLimit(queryParameters),
      prepareScrollId(queryParameters),
      sortKey,
      sortOrder
    )

    validatePagingParams(pagingParams)
    pagingParams
  }

  def scoringParamSet(queryParameters: MultiQueryParams): ScoringParamSet = {
    ScoringParamSet(
      prepareFieldBoosts(queryParameters),
      prepareDatatypeBoosts(queryParameters),
      prepareDomainBoosts(queryParameters),
      prepareOfficialBoost(queryParameters),
      prepareMinShouldMatch(queryParameters),
      prepareSlop(queryParameters),
      prepareAgeDecay(queryParameters)
    )
  }

  def formatParamSet(queryParameters: MultiQueryParams): FormatParamSet = {
    FormatParamSet(
      prepareShowScore(queryParameters),
      prepareShowVisiblity(queryParameters),
      prepareLocale(queryParameters)
    )
  }

  ////////
  // APPLY
  //
  // NOTE: Watch out for case sensitivity in params
  // Some field values are stored internally in lowercase, others are not
  // Yes, the params parser now concerns itself with ES internals
  def apply(queryParameters: MultiQueryParams): ValidatedQueryParameters = {
    val searchParams = searchParamSet(queryParameters)
    val scoringParams = scoringParamSet(queryParameters)
    val pagingParams = pagingParamSet(queryParameters)
    val formatParams = formatParamSet(queryParameters)
    ValidatedQueryParameters(searchParams, scoringParams, pagingParams, formatParams)
  }

  def prepUserParams(queryParameters: MultiQueryParams): ValidatedUserQueryParameters = {
    val searchParams = UserSearchParamSet(
      prepareId(queryParameters),
      prepareEmail(queryParameters),
      prepareScreenName(queryParameters),
      prepareFlag(queryParameters),
      prepareRoleName(queryParameters),
      prepareRoleId(queryParameters),
      prepareUserType(queryParameters),
      prepareUserDomain(queryParameters),
      prepareUserQuery(queryParameters)
    )

    val scoringParams = UserScoringParamSet(prepareMinShouldMatch(queryParameters))

    val (sortKey, sortOrder) = prepareSort(queryParameters)
    val pagingParams = PagingParamSet(
      prepareOffset(queryParameters),
      prepareLimit(queryParameters),
      None, // scrollId
      sortKey,
      sortOrder
    )

    validatePagingParams(pagingParams)

    ValidatedUserQueryParameters(searchParams, scoringParams, pagingParams)
  }
}

object Params {
  def arrayify(param: String): String = s"${param}[]"

  // catalog params
  val searchContext = "search_context"
  val domains = "domains"
  val categories = "categories"
  val tags = "tags"
  val only = "only"
  val forUser = "for_user"
  val attribution = "attribution"
  val provenance = "provenance"
  val derivedFrom = "derived_from"
  val sharedTo = "shared_to"
  val public = "public"
  val published = "published"
  val derived = "derived"
  val explicitlyHidden = "explicitly_hidden"
  val approvalStatus = "approval_status"
  val visibility = "visibility"
  val license = "license"
  val columnNames = "column_names"
  val submitterId = "submitter_id"
  val reviewerId = "reviewer_id"
  val reviewedAutomatically = "reviewed_automatically"

  val qInternal = "q_internal"
  val q = "q"

  // user search params
  val ids = "ids"
  val emails = "emails"
  val screenNames = "screen_names"
  val flags = "flags"
  val roles = "roles"
  val roleIds = "role_ids"
  val domain = "domain"

  // We allow catalog datatypes to be boosted using the boost{typename}={factor}
  // (eg. boostDatasets=10.0) syntax. To avoid redundancy, we get the available
  // types by way of the com.socrata.cetera.types.Datatypes.all method.
  // See com.socrata.cetera.types.Datatypes for the type definitions.
  //
  // Available datatype boosting parameters:
  //   boostCalendars
  //   boostDatalenses
  //   boostDatasets
  //   boostFiles
  //   boostFilters
  //   boostForms
  //   boostPulses
  //   boostStories
  //   boostLinks
  //   boostCharts
  //   boostMaps

  private val boostParamPrefix = "boost"

  private val datatypeBoostParams = Datatypes.all.map(datatype => s"$boostParamPrefix${datatype.plural.capitalize}")

  private def typeNameFromBoostParam(boostParam: String): Option[String] =
    Some(boostParam).collect { case s: String if s.startsWith(boostParamPrefix) => s.stripPrefix(boostParamPrefix) }

  def datatypeBoostParam(param: String): Option[Datatype] =
    datatypeBoostParams.find(_ == param).flatMap { boostParam =>
      Datatype(typeNameFromBoostParam(boostParam.toLowerCase))
    }

  // field boosting parameters
  val boostColumns = boostParamPrefix + "Columns"
  val boostDescription = boostParamPrefix + "Desc"
  val boostTitle = boostParamPrefix + "Title"

  // e.g., boostDomains[example.com]=1.23&boostDomains[data.seattle.gov]=4.56
  val boostDomains = boostParamPrefix + "Domains"

  val boostOfficial = boostParamPrefix + "Official"
  val minShouldMatch = "min_should_match"
  val slop = "slop"
  val ageDecay = "age_decay"

  // result formatting parameters
  val locale = "locale"
  val showScore = "show_score"
  val showVisibility = "show_visibility"

  // sorting and pagination parameters
  val limit = "limit"
  val offset = "offset"
  val scrollId = "scroll_id"
  val order = "order"

  ///////////////////////
  // Explicit Param Lists
  ///////////////////////

  // HEY! when adding/removing parameters update `catalog{String,Array,Map}Keys`  as appropriate.
  // These are used to distinguish catalog keys from custom metadata fields

  // If your param is a simple key/value pair, add it here
  private val catalogStringKeys = Set(
    attribution,
    provenance,
    searchContext,
    domains,
    categories,
    tags,
    only,
    forUser,
    derivedFrom,
    ids,
    public,
    published,
    derived,
    explicitlyHidden,
    approvalStatus,
    visibility,
    locale,
    sharedTo,
    qInternal,
    q,
    boostColumns,
    boostDescription,
    boostTitle,
    boostOfficial,
    minShouldMatch,
    slop,
    ageDecay,
    showScore,
    showVisibility,
    limit,
    offset,
    scrollId,
    order,
    license,
    columnNames,
    submitterId,
    reviewerId,
    reviewedAutomatically
  ) ++ datatypeBoostParams.toSet


  // If your param is an array like tags[]=fun&tags[]=ice+cream, add it here
  private val catalogArrayKeys = Set(
    arrayify(categories),
    arrayify(only),
    arrayify(tags),
    arrayify(columnNames)
  )

  // If your param is a hashmap like boostDomains[example.com]=1.23, add it here
  private val catalogMapKeys = Set(
    boostDomains
  )

  // For example: search_context, tags[], boostDomains[example.com]
  def isCatalogKey(key: String): Boolean = {
    catalogStringKeys.contains(key) ||
      catalogArrayKeys.contains(key) ||
      key.endsWith("]") && catalogMapKeys.contains(key.split('[').head)
  }

  // Remaining keys are treated as custom metadata fields and filtered on
  def remaining(qs: MultiQueryParams): MultiQueryParams = {
    qs.filterKeys { key => !isCatalogKey(key) }
  }
}
