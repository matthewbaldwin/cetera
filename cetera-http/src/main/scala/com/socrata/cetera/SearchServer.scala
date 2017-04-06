package com.socrata.cetera

import java.util.concurrent.{ExecutorService, Executors}

import com.rojoma.simplearm.v2._
import com.socrata.http.client.{HttpClientHttpClient, InetLivenessChecker}
import com.socrata.http.server.{HttpRequest, HttpResponse, SocrataServerJetty}
import com.socrata.thirdparty.typesafeconfig.Propertizer
import com.typesafe.config.ConfigFactory
import org.apache.log4j.PropertyConfigurator
import org.elasticsearch.client.transport.TransportClient
import org.slf4j.{LoggerFactory, MDC}

import com.socrata.cetera.auth.CoreClient
import com.socrata.cetera.config.CeteraConfig
import com.socrata.cetera.handlers.{AgeDecayParamSet, Router}
import com.socrata.cetera.metrics.BalboaClient
import com.socrata.cetera.search.{DocumentClient, DomainClient, ElasticSearchClient, UserClient, ScriptScoreFunction}
import com.socrata.cetera.services._

// $COVERAGE-OFF$ jetty wiring
object SearchServer extends App {
  val config = new CeteraConfig(ConfigFactory.load())
  PropertyConfigurator.configure(Propertizer("log4j", config.log4j))

  val logger = LoggerFactory.getLogger(getClass)

  logger.info("Starting Cetera server on port {}... ", config.server.port)
  logger.info("Configuration:\n" + config.debugString)

  implicit def executorResource[A <: ExecutorService]: Resource[A] = new Resource[A] {
    def close(a: A): Unit = {
      a.shutdown()
    }
  }

  trait Startable { def start(): Unit }
  def managedStartable[T <: Startable : Resource](resource: => T): Managed[T] = new Managed[T] {
    override def run[A](f: T => A): A =
      using(resource) { r =>
        r.start()
        f(r)
      }
  }

  for {
    executor <- managed(Executors.newCachedThreadPool())

    livenessChecker <- managedStartable(
      new InetLivenessChecker(config.http.liveness.interval,
                              config.http.liveness.range,
                              config.http.liveness.missable,
                              executor) with Startable)

    esClient <- managed(
      ElasticSearchClient(config.elasticSearch))

    httpClient <- managed(
      new HttpClientHttpClient(executor, HttpClientHttpClient.defaultOptions.withUserAgent("cetera")))
    } {
    val coreClient = new CoreClient(httpClient, config.core.host, config.core.port,
      config.core.connectionTimeoutMs, config.core.appToken)
    val domainClient = new DomainClient(esClient, coreClient, config.elasticSearch.indexAliasName)
    val documentClient = new DocumentClient(
      esClient,
      domainClient,
      config.elasticSearch.indexAliasName,
      config.elasticSearch.titleBoost,
      config.elasticSearch.minShouldMatch,
      config.elasticSearch.functionScoreScripts.flatMap(fnName =>
        ScriptScoreFunction(fnName)).toSet,
      config.elasticSearch.ageDecay.map(config =>
        AgeDecayParamSet(config.decayType, config.scale, config.decay, config.offset))
    )
    val userClient = new UserClient(esClient, config.elasticSearch.indexAliasName)

    logger.info("ElasticSearchClient initialized on nodes " +
                  esClient.client.asInstanceOf[TransportClient].transportAddresses().toString)

    logger.info("Initializing BalboClient")
    val balboaClient = new BalboaClient(config.balboa.dataDirectory)

    logger.info("Initializing VersionService")
    val versionService = VersionService

    logger.info("Initializing SearchService with document, domain, balboa, and verification clients")
    val searchService = new SearchService(documentClient, domainClient, balboaClient, coreClient)

    logger.info("Initializing AutocompleteService with document, domain, and core clients")
    val autocompleteService = new AutocompleteService(documentClient, domainClient, coreClient)

    logger.info("Initializing FacetService with document, domain, and verification clients")
    val facetService = new FacetService(documentClient, domainClient, coreClient)

    logger.info("Initializing DomainCountService with domain and verification clients")
    val domainCountService = new DomainCountService(domainClient, coreClient)

    logger.info("Initializing CountService with document, domain, and verification clients")
    val countService = new CountService(documentClient, domainClient, coreClient)

    logger.info("Initializing UserSearchService with user, verification, and domain clients")
    val userSearchService = new UserSearchService(userClient, domainClient, coreClient)

    logger.info("Initializing router with services")
    val router = new Router(
      versionService.Service,
      searchService.Service,
      autocompleteService.Service,
      facetService.Service,
      domainCountService.Service,
      countService.Service,
      userSearchService.Service
    )

    logger.info("Initializing handler")
    val handler: HttpRequest => HttpResponse = (req: HttpRequest) => {
      MDC.put("requestId", req.requestId)
      router.route(req)
    }

    logger.info("Initializing server")
    val server = new SocrataServerJetty(
      handler,
      SocrataServerJetty
        .defaultOptions
        .withPort(config.server.port)
        .withGracefulShutdownTimeout(config.server.gracefulShutdownTimeout)
    )

    logger.info("Running server!")
    server.run()
  }

  logger.info("All done!")
}
// $COVERAGE-ON$
