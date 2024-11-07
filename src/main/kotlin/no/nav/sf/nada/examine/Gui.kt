package no.nav.sf.nada.examine

import com.google.gson.JsonObject
import com.google.gson.JsonParser
import mu.KotlinLogging
import no.nav.sf.nada.Bootstrap
import no.nav.sf.nada.Metrics
import no.nav.sf.nada.addYesterdayRestriction
import no.nav.sf.nada.doSFQuery
import no.nav.sf.nada.gson
import no.nav.sf.nada.token.AccessTokenHandler
import org.http4k.core.HttpHandler
import org.http4k.core.Request
import org.http4k.core.Response
import org.http4k.core.Status
import java.io.File

object Gui {
    private val log = KotlinLogging.logger { }

    val htmlForConfigHandler: HttpHandler = {
        var htmlTemplate = "<div id=\"config\" style=\"font-size:14px\n" +
            "    ; font-weight:bold\">PROJECTID<br>POST_TO_BIGQUERY<br>RUN_SESSION_ON_STARTUP<br>FETCH_ALL_RECORDS<br>EXCLUDE_TABLES</div>"
        htmlTemplate = htmlTemplate
            .replace("PROJECTID", "Project id: ${Bootstrap.projectId}")
            .replace("POST_TO_BIGQUERY", "Post to bigquery: ${Bootstrap.postToBigQuery}")
            .replace("RUN_SESSION_ON_STARTUP", "Run session on startup: ${Bootstrap.runSessionOnStartup}")
            .replace("FETCH_ALL_RECORDS", "Fetch all records: ${Bootstrap.fetchAllRecords}")
            .replace("EXCLUDE_TABLES", "Skip fetching records for the following tables: ${Bootstrap.excludeTables}")
        Response(Status.OK).body(htmlTemplate)
    }

    val datasetsHandler: HttpHandler = {
        Response(Status.OK).body(gson.toJson(Bootstrap.mapDef.keys))
    }

    val tablesHandler: HttpHandler = {
        val dataset = it.query("dataset")
        Response(Status.OK).body(gson.toJson(Bootstrap.mapDef[dataset]!!.keys))
    }

    val schemaMapHandler: HttpHandler = { req: Request ->
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
    }

    val testCallHandler: HttpHandler = { req: Request ->
        log.info { "Will perform testcall " }
        File("/tmp/latesttestcallrequest").writeText(req.toMessage())
        var result = ""
        val dataset = req.query("dataset")
        val table = req.query("table")
        val query = Bootstrap.mapDef[dataset]!![table]!!.query

        val queryYesterday = query.addYesterdayRestriction()

        val responseTotal = doSFQuery("${AccessTokenHandler.instanceUrl}${Bootstrap.SF_QUERY_BASE}$query")
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
        val responseDate = doSFQuery("${AccessTokenHandler.instanceUrl}${Bootstrap.SF_QUERY_BASE}$queryYesterday")
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
    }
}
