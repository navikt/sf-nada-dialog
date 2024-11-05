package no.nav.kafka.dialog

import com.google.gson.JsonObject
import com.google.gson.JsonParser
import io.prometheus.client.Gauge
import io.prometheus.client.exporter.common.TextFormat
import mu.KotlinLogging
import no.nav.sf.nada.Bootstrap
import no.nav.sf.nada.Bootstrap.SF_QUERY_BASE
import no.nav.sf.nada.Bootstrap.excludeTables
import no.nav.sf.nada.Bootstrap.fetchAllRecords
import no.nav.sf.nada.Bootstrap.postToBigQuery
import no.nav.sf.nada.Bootstrap.projectId
import no.nav.sf.nada.Bootstrap.runSessionOnStartup
import no.nav.sf.nada.Metrics
import no.nav.sf.nada.Metrics.cRegistry
import no.nav.sf.nada.addYesterdayRestriction
import no.nav.sf.nada.bulk.BulkOperation
import no.nav.sf.nada.doSFBulkJobStatusQuery
import no.nav.sf.nada.doSFBulkStartQuery
import no.nav.sf.nada.doSFQuery
import no.nav.sf.nada.gson
import no.nav.sf.nada.token.AccessTokenHandler
import org.http4k.core.HttpHandler
import org.http4k.core.Method
import org.http4k.core.Request
import org.http4k.core.Response
import org.http4k.core.Status
import org.http4k.routing.ResourceLoader.Companion.Classpath
import org.http4k.routing.bind
import org.http4k.routing.routes
import org.http4k.routing.static
import org.http4k.server.ApacheServer
import org.http4k.server.asServer
import java.io.File
import java.io.StringWriter

private val log = KotlinLogging.logger { }

fun naisAPI(): HttpHandler = routes(
    "/internal/examine" bind Method.GET to static(Classpath("examine")),
    "/internal/bulk" bind Method.GET to static(Classpath("bulk")),
    "/internal/htmlforconfig" bind Method.GET to {
        var htmlTemplate = "<div id=\"config\" style=\"font-size:14px\n" +
            "    ; font-weight:bold\">PROJECTID<br>POST_TO_BIGQUERY<br>RUN_SESSION_ON_STARTUP<br>FETCH_ALL_RECORDS<br>EXCLUDE_TABLES</div>"
        htmlTemplate = htmlTemplate
            .replace("PROJECTID", "Project id: $projectId")
            .replace("POST_TO_BIGQUERY", "Post to bigquery: $postToBigQuery")
            .replace("RUN_SESSION_ON_STARTUP", "Run session on startup: $runSessionOnStartup")
            .replace("FETCH_ALL_RECORDS", "Fetch all records: $fetchAllRecords")
            .replace("EXCLUDE_TABLES", "Skip fetching records for the following tables: $excludeTables")
        Response(Status.OK).body(htmlTemplate)
    },
    "/internal/datasets" bind Method.GET to {
        Response(Status.OK).body(gson.toJson(Bootstrap.mapDef.keys))
    },
    "/internal/tables" bind Method.GET to { req: Request ->
        val dataset = req.query("dataset")
        Response(Status.OK).body(gson.toJson(Bootstrap.mapDef[dataset]!!.keys))
    },
    "/internal/schemamap" bind Method.GET to { req: Request ->
        val dataset = req.query("dataset")
        val table = req.query("table")
        val query = Bootstrap.mapDef[dataset]!![table]!!.query
        val fieldDefMap = Bootstrap.mapDef[dataset]!![table]!!.fieldDefMap
        var result = ""
        result += "<h4>Will use query:</h4>\n $query\n\n"
        result += "<h4>Expected keys on record:</h4>\n"
        result += fieldDefMap.keys.joinToString("\n") + "\n\n"
        result += "<h4>Will be mapped to table row:</h4>\n"
        result += fieldDefMap.values.map { "${it.name} (${it.type})" }.joinToString("\n")

        Response(Status.OK).body(result)
    },
    "/internal/testcall" bind Method.GET to { req: Request ->
        log.info { "Will perform testcall " }
        File("/tmp/latesttestcallrequest").writeText(req.toMessage())
        var result = ""
        val dataset = req.query("dataset")
        val table = req.query("table")
        val query = Bootstrap.mapDef[dataset]!![table]!!.query

        val queryYesterday = query.addYesterdayRestriction()

        val responseTotal = doSFQuery("${AccessTokenHandler.instanceUrl}$SF_QUERY_BASE$query")
        File("/tmp/responseAtTotalCall").writeText(responseTotal.toMessage())
        if (responseTotal.status.code == 400) {
            result += "Bad request: " + responseTotal.bodyString()
            File("/tmp/badRequestAtTotalCall").writeText(responseTotal.bodyString())
        } else {
            try {
                val obj = JsonParser.parseString(responseTotal.bodyString()) as JsonObject
                val totalSize = obj["totalSize"].asInt
                Metrics.latestTotalFromTestCall.labels(table).set(totalSize.toDouble())
                result = "Total number of records found is $totalSize"
            } catch (e: Exception) {
                result += e.message
                File("/tmp/exceptionAtTotalCall").writeText(e.toString() + "\n" + e.stackTraceToString())
            }
        }
        val responseDate = doSFQuery("${AccessTokenHandler.instanceUrl}$SF_QUERY_BASE$queryYesterday")
        File("/tmp/responseAtDateCall").writeText(responseDate.toMessage())
        if (responseTotal.status.code == 400) {
            result += "\nBad request: " + responseDate.bodyString()
            File("/tmp/badRequestAtDateCall").writeText(responseDate.bodyString())
        } else {
            try {
                val obj = JsonParser.parseString(responseDate.bodyString()) as JsonObject
                val totalSize = obj["totalSize"].asInt
                result += "\nNumber of records from yesterday poll $totalSize"
            } catch (e: Exception) {
                result += "\n" + e.message
                File("/tmp/exceptionAtDateCall").writeText(e.toString() + "\n" + e.stackTraceToString())
            }
        }
        Response(Status.OK).body(result)
    },
    "/internal/isAlive" bind Method.GET to { Response(Status.OK) },
    "/internal/isReady" bind Method.GET to { Response(Status.OK) },
    "/internal/metrics" bind Method.GET to {
        runCatching {
            StringWriter().let { str ->
                TextFormat.write004(str, cRegistry.metricFamilySamples())
                str
            }.toString()
        }
            .onFailure {
                log.error { "/prometheus failed writing metrics - ${it.localizedMessage}" }
            }
            .getOrDefault("")
            .responseByContent()
    },
    "/internal/stop" bind Method.GET to {
        preStopHook.inc()
        PrestopHook.activate()
        log.info { "Received PreStopHook from NAIS" }
        Response(Status.OK)
    },
    "/internal/performBulk" bind Method.GET to {
        if (!BulkOperation.operationIsActive) {
            val dataset = it.query("dataset")
            val table = it.query("table")
            BulkOperation.dataset = dataset!!
            BulkOperation.table = table!!

            val bulkResponse = doSFBulkStartQuery(BulkOperation.dataset, BulkOperation.table)

            try {
                val responseObj = JsonParser.parseString(bulkResponse.bodyString()) as JsonObject
                BulkOperation.jobId = responseObj["id"].asString
                BulkOperation.operationIsActive = true
                // Response(Status.OK).body(bulkResponse.bodyString())
            } catch (e: Exception) {
                log.error { e.stackTraceToString() }
                Response(Status.OK).body("Something went wrong with bulk start, response from SF ${bulkResponse.status.code} ${bulkResponse.bodyString()}")
            }
        }
        val bulkJobStatusResponse = doSFBulkJobStatusQuery(BulkOperation.jobId)
        try {
            val responseObj = JsonParser.parseString(bulkJobStatusResponse.bodyString()) as JsonObject
            BulkOperation.jobId = responseObj["id"].asString
            BulkOperation.operationIsActive = true
        } catch (e: Exception) {
            log.error { e.stackTraceToString() }
            Response(Status.OK).body("Something went wrong with check, response from SF ${bulkJobStatusResponse.status.code} ${bulkJobStatusResponse.bodyString()}")
        }
        Response(Status.OK).body(bulkJobStatusResponse.bodyString())
    },
    "/internal/reconnect" bind Method.GET to {
        val id = it.query("id")
        BulkOperation.jobId = id!!
        log.info { "Reconnecting gui to jobId $id" }
        Response(Status.OK).body("Reconnected to jobId $id")
    }
)

fun enableNAISAPI(port: Int = 8080, doSomething: () -> Unit): Boolean =
    naisAPI().asServer(ApacheServer(port)).let { srv ->
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

private fun String.responseByContent(): Response =
    if (this.isNotEmpty()) Response(Status.OK).body(this) else Response(Status.NO_CONTENT)

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

internal val preStopHook: Gauge = Gauge
    .build()
    .name("pre_stop__hook_gauge")
    .help("No. of preStopHook activations since last restart")
    .register()

object PrestopHook {

    private val log = KotlinLogging.logger { }

    @Volatile
    private var prestopHook = false

    init {
        log.info { "Installing prestop hook" }
    }

    fun isActive() = prestopHook
    fun activate() { prestopHook = true }
    fun reset() { prestopHook = false }
}
