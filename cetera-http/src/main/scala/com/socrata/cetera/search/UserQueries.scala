package com.socrata.cetera.search

import org.apache.lucene.search.join.ScoreMode
import org.apache.lucene.queryparser.flexible.standard.QueryParserUtil
import org.elasticsearch.index.query._
import org.elasticsearch.index.query.QueryBuilders._

import com.socrata.cetera.auth.{AuthedUser, User}
import com.socrata.cetera.errors.UnauthorizedError
import com.socrata.cetera.handlers.UserSearchParamSet
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

  def roleQuery(roles: Option[Set[String]]): Option[TermsQueryBuilder] =
    roles.map(r => termsQuery(UserRole.fieldName, r.toSeq: _*))

  def domainQuery(domainId: Option[Int]): Option[TermQueryBuilder] =
    domainId.map(d => termQuery(UserDomainId.fieldName, d))

  def nestedRolesQuery(roles: Option[Set[String]], domainId: Option[Int]): Option[NestedQueryBuilder] = {
    val queries = Seq(domainQuery(domainId), roleQuery(roles)).flatten
    if (queries.isEmpty) {
      None
    } else {
      val path = "roles"
      val query = queries.foldLeft(boolQuery()) { (b, f) => b.must(f) }
      Some(nestedQuery(path, query, ScoreMode.Avg))
    }
  }

  def authQuery(user: Option[AuthedUser], domain: Option[Domain]): Option[NestedQueryBuilder] = {
    user match {
      // no restrictions are needed for super admins
      case Some(u) if (u.isSuperAdmin) => None
      // if the user is searching for users on a domain, it must be the domain they are authed on
      case Some(u) if (domain.exists(d => d.domainId != u.authenticatingDomain.domainId)) =>
        throw UnauthorizedError(Some(u.id), s"search for users on domain ${domain.get.domainCname}")
      // if the user can view all users, no restrictions are needed (other than the one above)
      case Some(u) if (u.canViewAllUsers) => None
      // if the user can view domain users, we restrict the user search to user's authenticating domain
      case Some(u) if (u.canViewUsersOnDomain(u.authenticatingDomain.domainId)) =>
        nestedRolesQuery(None, Some(u.authenticatingDomain.domainId))
      // if the user can't view users or is not authenticated, we throw
      case _ => throw UnauthorizedError(user.map(_.id), "search users")
    }
  }

  def compositeQuery(
      searchParams: UserSearchParamSet,
      domain: Option[Domain],
      authorizedUser: Option[AuthedUser])
  : QueryBuilder = {
    val queries = Seq(
      idQuery(searchParams.ids),
      emailQuery(searchParams.emails),
      screenNameQuery(searchParams.screenNames),
      flagQuery(searchParams.flags),
      nestedRolesQuery(searchParams.roles, domain.map(_.domainId)),
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
      authorizedUser: Option[AuthedUser])
  : BoolQueryBuilder = {
    val emailOrNameQuery = emailNameMatchQuery(searchParams.query)
    val userQuery = compositeQuery(searchParams, domain, authorizedUser)
    boolQuery.filter(userQuery).must(emailOrNameQuery)
  }
}
