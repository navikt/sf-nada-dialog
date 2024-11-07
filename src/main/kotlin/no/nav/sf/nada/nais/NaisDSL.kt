package no.nav.kafka.dialog

import mu.KotlinLogging
import no.nav.sf.nada.Metrics
import no.nav.sf.nada.bulk.BulkOperation
import no.nav.sf.nada.examine.Gui
import org.http4k.core.HttpHandler
import org.http4k.core.Method
import org.http4k.core.Response
import org.http4k.core.Status
import org.http4k.routing.ResourceLoader.Companion.Classpath
import org.http4k.routing.bind
import org.http4k.routing.routes
import org.http4k.routing.static
import org.http4k.server.ApacheServer
import org.http4k.server.asServer

private val log = KotlinLogging.logger { }

fun api(): HttpHandler = routes(
    "/internal/examine" bind Method.GET to static(Classpath("examine")),
    "/internal/bulk" bind Method.GET to static(Classpath("bulk")),
    "/internal/htmlforconfig" bind Gui.htmlForConfigHandler,
    "/internal/datasets" bind Method.GET to Gui.datasetsHandler,
    "/internal/tables" bind Method.GET to Gui.tablesHandler,
    "/internal/schemamap" bind Method.GET to Gui.schemaMapHandler,
    "/internal/testcall" bind Method.GET to Gui.testCallHandler,
    "/internal/isAlive" bind Method.GET to { Response(Status.OK) },
    "/internal/isReady" bind Method.GET to { Response(Status.OK) },
    "/internal/metrics" bind Method.GET to Metrics.metricsHandler,
    "/internal/performBulk" bind Method.GET to BulkOperation.performBulkHandler,
    "/internal/reconnect" bind Method.GET to BulkOperation.reconnectHandler,
    "/internal/activeId" bind Method.GET to BulkOperation.activeIdHandler,
    "/internal/transfer" bind Method.GET to BulkOperation.transferHandler
)

fun apiServer(port: Int = 8080) = api().asServer(ApacheServer(port))

fun enableNAISAPI(port: Int = 8080, doSomething: () -> Unit): Boolean =
    api().asServer(ApacheServer(port)).let { srv ->
        try {
            srv.start().use {
                log.info { "NAIS DSL is up and running at port $port" }
                runCatching(doSomething)
                    .onFailure {
                        log.error { "Failure during run inside enableNAISAPI - ${it.localizedMessage} Stack: ${it.printStackTrace()}" }
                    }
            }
            true
        } catch (e: Exception) {
            log.error { "Failure during enable/disable NAIS api for port $port - ${e.localizedMessage}" }
            false
        } finally {
            srv.close()
            log.info { "NAIS DSL is stopped at port $port" }
        }
    }

object ShutdownHook {

    private val log = KotlinLogging.logger { }

    @Volatile
    private var shutdownhookActiveOrOther = false
    private val mainThread: Thread = Thread.currentThread()

    init {
        log.info { "Installing shutdown hook" }
        Runtime.getRuntime()
            .addShutdownHook(
                object : Thread() {
                    override fun run() {
                        shutdownhookActiveOrOther = true
                        log.info { "shutdown hook activated" }
                        mainThread.join()
                    }
                })
    }

    fun isActive() = shutdownhookActiveOrOther
    fun reset() { shutdownhookActiveOrOther = false }
}
