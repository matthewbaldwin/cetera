package com.socrata.cetera.search

import org.apache.lucene.search.join.ScoreMode
import org.apache.lucene.queryparser.flexible.standard.QueryParserUtil
import org.elasticsearch.index.query._
import org.elasticsearch.index.query.QueryBuilders._

import com.socrata.cetera.auth.{AuthedUser, User}
import com.socrata.cetera.errors.{MissingRequiredParameterError, UnauthorizedError}
import com.socrata.cetera.handlers.{UserScoringParamSet, UserSearchParamSet}
import com.socrata.cetera.types._
import com.socrata.cetera.esUserType

object UserQueries {
  def idQuery(ids: Option[Set[String]]): Option[IdsQueryBuilder] =
    ids.map(i => idsQuery().types(esUserType).addIds(i.toSeq: _*))

  def emailQuery(emails: Option[Set[String]]): Option[TermsQueryBuilder] =
    emails.map(e => termsQuery(UserEmail.rawFieldName, e.map(_.toLowerCase).toSeq: _*))

  def screenNameQuery(screenNames: Option[Set[String]]): Option[TermsQueryBuilder] =
    screenNames.map(s => termsQuery(UserScreenName.rawFieldName, s.map(_.toLowerCase).toSeq: _*))

  def flagQuery(flags: Option[Set[String]]): Option[TermsQueryBuilder] =
    flags.map(r => termsQuery(UserFlag.fieldName, r.toSeq: _*))

  def roleNameQuery(roles: Option[Set[String]]): Option[TermsQueryBuilder] =
    roles.map(r => termsQuery(UserRoleName.fieldName, r.toSeq: _*))

  def roleIdQuery(roleIds: Option[Set[Int]]): Option[TermsQueryBuilder] =
    roleIds.map(r => termsQuery(UserRoleId.fieldName, r.toSeq: _*))

  def domainQuery(domainId: Option[Int]): Option[TermQueryBuilder] =
    domainId.map(d => termQuery(UserDomainId.fieldName, d))

  def nestedRoleQuery(roleQuery: Option[TermsQueryBuilder], domainId: Option[Int]): Option[NestedQueryBuilder] = {
    val queries = Seq(domainQuery(domainId), roleQuery).flatten
    if (queries.isEmpty) {
      None
    } else {
      val path = "roles"
      val query = queries.foldLeft(boolQuery()) { (b, f) => b.must(f) }
      Some(nestedQuery(path, query, ScoreMode.Avg))
    }
  }

  def nestedRoleNamesQuery(roleNames: Option[Set[String]], domainId: Option[Int]): Option[NestedQueryBuilder] =
    nestedRoleQuery(roleNameQuery(roleNames), domainId)

  def nestedRoleIdsQuery(roleIds: Option[Set[Int]], domainId: Option[Int]): Option[NestedQueryBuilder] =
    nestedRoleQuery(roleIdQuery(roleIds), domainId)


  def authQuery(user: AuthedUser, domain: Option[Domain]): Option[NestedQueryBuilder] =
    if (user.isSuperAdmin) {
      // no restrictions are needed for super admins
      None
    } else if (domain.exists(d => d.id != user.authenticatingDomain.id)) {
      // if the user is searching for users on a domain, it must be the domain they are authed on
      throw UnauthorizedError(Some(user.id), s"search for users on domain ${domain.get.cname}")
    } else if (user.canViewAllUsers) {
      // if the user can view all users, no restrictions are needed (other than the one above)
      None
    } else if (user.canViewUsersOnDomain(user.authenticatingDomain.id)) {
      // if the user can view domain users, we restrict the user search to user's authenticating domain
      nestedRoleQuery(None, Some(user.authenticatingDomain.id))
    } else {
      // otherwise the user can't veiw any users
      throw UnauthorizedError(Some(user.id), "search users")
    }

  def compositeQuery(
      searchParams: UserSearchParamSet,
      domain: Option[Domain],
      authorizedUser: AuthedUser)
    : QueryBuilder = {

    val queries = Seq(
      idQuery(searchParams.ids),
      emailQuery(searchParams.emails),
      screenNameQuery(searchParams.screenNames),
      flagQuery(searchParams.flags),
      nestedRoleNamesQuery(searchParams.roleNames, domain.map(_.id)),
      nestedRoleIdsQuery(searchParams.roleIds, domain.map(_.id)),
      authQuery(authorizedUser, domain)
    ).flatten

    if (queries.isEmpty) {
      matchAllQuery()
    } else {
      queries.foldLeft(boolQuery()) { (b, f) => b.must(f) }
    }
  }

  def emailNameMatchQuery(query: Option[String]): QueryBuilder =
    query match {
      case None => matchAllQuery()
      case Some(q) =>
        val sanitizedQ = QueryParserUtil.escape(q)
        queryStringQuery(sanitizedQ)
          .field(UserScreenName.fieldName)
          .field(UserScreenName.rawFieldName)
          .field(UserEmail.fieldName)
          .field(UserEmail.rawFieldName)
          .autoGeneratePhraseQueries(true)
    }

  def userQuery(
      searchParams: UserSearchParamSet,
      domain: Option[Domain],
      authorizedUser: AuthedUser)
    : BoolQueryBuilder = {

    val emailOrNameQuery = emailNameMatchQuery(searchParams.query)
    val userQuery = compositeQuery(searchParams, domain, authorizedUser)
    boolQuery.filter(userQuery).must(emailOrNameQuery)
  }

  def namedEmailQuery(query: String): MatchQueryBuilder =
    matchQuery(UserEmail.autocompleteFieldName, query).queryName("email")

  def namedScreenNameQuery(query: String): MatchQueryBuilder =
    matchQuery(UserScreenName.autocompleteFieldName, query).queryName("screen_name")

  def autocompleteQuery(
      searchParams: UserSearchParamSet,
      scoringParams: UserScoringParamSet,
      domain: Option[Domain],
      authorizedUser: AuthedUser)
    : BoolQueryBuilder = {

    val msm = scoringParams.minShouldMatch.getOrElse("100%")

    val emailOrNameQuery = searchParams.query match {
      case Some(q) =>
        boolQuery()
          .should(namedEmailQuery(q).minimumShouldMatch(msm))
          .should(namedScreenNameQuery(q).minimumShouldMatch(msm))
      case _ => throw new MissingRequiredParameterError("q", "search query")
    }

    boolQuery.filter(compositeQuery(searchParams, domain, authorizedUser)).must(emailOrNameQuery)
  }
}
