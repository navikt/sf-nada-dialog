package no.nav.sf.nada

import com.google.gson.Gson
import com.google.gson.JsonObject
import com.google.gson.JsonParser
import kotlinx.coroutines.delay
import kotlinx.coroutines.launch
import kotlinx.coroutines.runBlocking
import mu.KotlinLogging
import no.nav.kafka.dialog.PrestopHook
import no.nav.kafka.dialog.ShutdownHook
import no.nav.sf.nada.token.AccessTokenHandler
import org.http4k.core.Method
import org.http4k.core.Request
import org.http4k.core.Response
import org.http4k.urlEncoded
import java.io.File
import java.time.LocalDate
import java.time.format.DateTimeFormatter
import kotlin.streams.toList

private val log = KotlinLogging.logger { }

val gson = Gson()

/**
 * conditionalWait
 * Interruptable wait function
 */
fun conditionalWait(ms: Long) =
    runBlocking {

        log.debug { "Will wait $ms ms" }

        val cr = launch {
            runCatching { delay(ms) }
                .onSuccess { log.debug { "waiting completed" } }
                .onFailure { log.info { "waiting interrupted" } }
        }

        tailrec suspend fun loop(): Unit = when {
            cr.isCompleted -> Unit
            ShutdownHook.isActive() || PrestopHook.isActive() -> cr.cancel()
            else -> {
                delay(250L)
                loop()
            }
        }

        loop()
        cr.join()
    }

fun ByteArray.encodeB64(): String = org.apache.commons.codec.binary.Base64.encodeBase64URLSafeString(this)
fun String.encodeB64UrlSafe(): String = org.apache.commons.codec.binary.Base64.encodeBase64URLSafeString(this.toByteArray())
fun String.encodeB64(): String = org.apache.commons.codec.binary.Base64.encodeBase64String(this.toByteArray())
fun String.decodeB64(): ByteArray = org.apache.commons.codec.binary.Base64.decodeBase64(this)

/**
 * Shortcuts for fetching environment variables
 */
fun env(env: String): String { return System.getenv(env) }

fun envAsLong(env: String): Long { return System.getenv(env).toLong() }

fun envAsInt(env: String): Int { return System.getenv(env).toInt() }

fun envAsBoolean(env: String): Boolean { return System.getenv(env).trim().toBoolean() }

fun envAsList(env: String): List<String> { return System.getenv(env).split(",").map { it.trim() }.toList() }

enum class Type { STRING, INTEGER, DATETIME, DATE, BOOL }
data class FieldDef(val name: String, val type: Type)
data class TableDef(val query: String, val fieldDefMap: MutableMap<String, FieldDef>)

fun parseMapDef(filePath: String): Map<String, Map<String, TableDef>> =
    parseMapDef(JsonParser.parseString(Bootstrap::class.java.getResource(filePath).readText()) as JsonObject)

fun parseMapDef(obj: JsonObject): Map<String, Map<String, TableDef>> {
    val result: MutableMap<String, MutableMap<String, TableDef>> = mutableMapOf()

    obj.entrySet().forEach { dataSetEntry ->
        val objDS = dataSetEntry.value.asJsonObject
        result[dataSetEntry.key] = mutableMapOf()
        objDS.entrySet().forEach { tableEntry ->
            val objT = tableEntry.value.asJsonObject
            val query = objT["query"]!!.asString.replace(" ", "+")
            val objS = objT["schema"]!!.asJsonObject
            result[dataSetEntry.key]!![tableEntry.key] = TableDef(query, mutableMapOf())
            objS.entrySet().forEach { fieldEntry ->
                val fieldDef = gson.fromJson(fieldEntry.value, FieldDef::class.java)
                result[dataSetEntry.key]!![tableEntry.key]!!.fieldDefMap[fieldEntry.key] = fieldDef
            }
        }
    }
    return result
}

fun doSFQuery(query: String): Response {
    val request = Request(Method.GET, "$query")
        .header("Authorization", "Bearer ${AccessTokenHandler.accessToken}")
        .header("Content-Type", "application/json;charset=UTF-8")
    // File("/tmp/queryToHappen").writeText(request.toMessage())
    val response = Bootstrap.client.value(request)
    // File("/tmp/responseThatHappend").writeText(response.toMessage())
    return response
}

fun doSFBulkStartQuery(dataset: String, table: String): Response {
    val query = Bootstrap.mapDef[dataset]!![table]!!.query.replace("+", " ")
    val request = Request(Method.POST, "${AccessTokenHandler.instanceUrl}/services/data/v57.0/jobs/query")
        .header("Authorization", "Bearer ${AccessTokenHandler.accessToken}")
        .header("Content-Type", "application/json;charset=UTF-8")
        .body(
            """{
                "operation": "query",
                "query": "$query",
                "contentType": "CSV"
                  }""".trim()
        )

    File("/tmp/bulkQueryToHappen").writeText(request.toMessage())
    val response = Bootstrap.client.value(request)
    File("/tmp/bulkResponseThatHappend").writeText(response.toMessage())
    return response
}

fun doSFBulkJobStatusQuery(jobId: String): Response {
    val request = Request(Method.GET, "${AccessTokenHandler.instanceUrl}/services/data/v57.0/jobs/query/$jobId")
        .header("Authorization", "Bearer ${AccessTokenHandler.accessToken}")
        .header("Content-Type", "application/json;charset=UTF-8")
    File("/tmp/bulkJobStatusQueryToHappen").writeText(request.toMessage())
    val response = Bootstrap.client.value(request)
    File("/tmp/bulkJobStatusResponseThatHappend").writeText(response.toMessage())
    return response
}

fun doSFBulkJobResultQuery(jobId: String): Response {
    // GET /services/data/v57.0/jobs/query/<jobID>/results
    val request = Request(Method.GET, "${AccessTokenHandler.instanceUrl}/services/data/v57.0/jobs/query/$jobId/results")
        .header("Authorization", "Bearer ${AccessTokenHandler.accessToken}")
        .header("Content-Type", "application/json;charset=UTF-8")

    val response = Bootstrap.client.value(request)
    File("/tmp/bulkJobResultResponse").writeText(response.toMessage())
    return response
}

fun String.addDateRestriction(localDate: LocalDate): String {
    val connector = if (this.contains("WHERE")) "+AND" else "+WHERE"
    return this + "$connector+LastModifiedDate>=TODAY+AND+LastModifiedDate<=TOMORROW"
        .replace("TODAY", "${localDate.format(DateTimeFormatter.ISO_DATE)}T00:00:00Z".urlEncoded())
        .replace("TOMORROW", "${localDate.plusDays(1).format(DateTimeFormatter.ISO_DATE)}T00:00:00Z".urlEncoded())
        .replace(">", ">".urlEncoded())
        .replace("<", "<".urlEncoded())
}

fun String.addYesterdayRestriction(): String = this.addDateRestriction(LocalDate.now().minusDays(1))
