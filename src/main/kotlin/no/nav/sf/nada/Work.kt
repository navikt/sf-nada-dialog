package no.nav.sf.nada

import com.google.cloud.bigquery.InsertAllRequest
import com.google.cloud.bigquery.TableId
import com.google.gson.JsonArray
import com.google.gson.JsonElement
import com.google.gson.JsonNull
import com.google.gson.JsonObject
import com.google.gson.JsonParser
import mu.KotlinLogging
import no.nav.sf.nada.Bootstrap.SF_QUERY_BASE
import no.nav.sf.nada.Bootstrap.bigQueryService
import no.nav.sf.nada.Bootstrap.excludeTables
import no.nav.sf.nada.Bootstrap.fetchAllRecords
import no.nav.sf.nada.Bootstrap.hasPostedToday
import no.nav.sf.nada.Bootstrap.mapDef
import no.nav.sf.nada.Bootstrap.postToBigQuery
import no.nav.sf.nada.token.AccessTokenHandler
import org.http4k.core.Response
import java.io.File
import java.lang.IllegalStateException
import java.lang.RuntimeException
import java.time.LocalDate

private val log = KotlinLogging.logger {}

fun fetchAndSend(localDate: LocalDate?, dataset: String, table: String) {
    if (localDate == null) {
        log.warn { "No localDate for fetchAndSend specified - will fetch for dataset $dataset table $table without date constraints" }
    } else {
        log.info { "Will perform fetchAndSend for dataset $dataset table $table on localDate $localDate" }
    }
    if (!mapDef.containsKey(dataset)) {
        throw RuntimeException("mapDef.json is missing a definition for dataset $dataset")
    } else if (mapDef[dataset]?.containsKey(table) == false) {
        throw RuntimeException("mapDef.json is missing a definition for table $table in dataset $dataset")
    }
    val query = mapDef[dataset]!![table]!!.query.let { q ->
        if (localDate == null) q else q.addDateRestriction(localDate)
    }
    log.info { "Will use query: $query" }

    val fieldDef = mapDef[dataset]!![table]!!.fieldDefMap

    val tableId = TableId.of(dataset, table)

    Metrics.fetchRequest.inc()
    var response = doSFQuery("${AccessTokenHandler.instanceUrl}$SF_QUERY_BASE$query")
    // File("/tmp/latestresponsebody-$table").writeText(response.bodyString())
    var obj: JsonObject
    try {
        obj = JsonParser.parseString(response.bodyString()) as JsonObject
        File("/tmp/latestParsedObject").writeText(obj.toString())
    } catch (e: Exception) {
        val arr: JsonArray = JsonParser.parseString(response.bodyString()) as JsonArray
        File("/tmp/latestParsedArray").writeText(arr.toString())
        throw IllegalStateException(e.message)
    }
    val totalSize = obj["totalSize"].asInt
    var done = obj["done"].asBoolean
    var nextRecordsUrl: String? = obj["nextRecordsUrl"]?.asString
    log.info { "QUERY RESULT overview - totalSize $totalSize, done $done, nextRecordsUrl $nextRecordsUrl" }
    Metrics.productsRead.labels(table).inc(totalSize.toDouble())
    if (totalSize > 0) {
        var records = obj["records"].asJsonArray
        remapAndSendRecords(records, tableId, fieldDef)
        while (!done) {
            response = doSFQuery("${AccessTokenHandler.instanceUrl}$nextRecordsUrl")
            obj = JsonParser.parseString(response.bodyString()) as JsonObject
            done = obj["done"].asBoolean
            nextRecordsUrl = obj["nextRecordsUrl"]?.asString
            log.info { "CONTINUATION RESULT overview - totalSize $totalSize, done $done, nextRecordsUrl $nextRecordsUrl" }
            records = obj["records"].asJsonArray
            remapAndSendRecords(records, tableId, fieldDef)
        }
    }
}

fun Response.parsedRecordsCount(): Int {
    val obj = JsonParser.parseString(this.bodyString()) as JsonObject
    return obj["totalSize"].asInt
}

fun JsonObject.findBottomElement(defKey: String): JsonElement {
    val subKeys = defKey.split(".")
    val depth = subKeys.size
    if (depth == 1) return this[defKey]
    var element = this[subKeys[0]]
    for (i in 1 until subKeys.size) {
        element = (element as JsonObject)[subKeys[i]]
    }
    return element
}

fun remapAndSendRecords(records: JsonArray, tableId: TableId, fieldDefMap: MutableMap<String, FieldDef>) {
    val builder = InsertAllRequest.newBuilder(tableId)
    if (!postToBigQuery) {
        File("/tmp/translateProcess").writeText("") // Clear file
    }
    records.forEach { record ->
        builder.addRow((record as JsonObject).toRowMap(fieldDefMap))
    }
    val insertAllRequest = builder.build()
    records.last().let { File("/tmp/latestRecord_${tableId.table}").writeText("$it") }
    insertAllRequest.rows.last().let { File("/tmp/latestRow_${tableId.table}").writeText("$it") }
    insertAllRequest.rows.map { "$it" }.joinToString(",\n").let { File("/tmp/allRows_${tableId.table}").writeText("$it") }
    if (postToBigQuery) {
        val response = bigQueryService.insertAll(insertAllRequest)
        if (response.hasErrors()) {
            log.error { "Failure at insert: ${response.insertErrors}" }
            throw RuntimeException("Failure at insert: ${response.insertErrors}")
        } else {
            Metrics.productsSent.labels(tableId.table).inc(records.count().toDouble())
            log.info { "Rows (${records.count()}) successfully inserted into dataset ${tableId.dataset}, table ${tableId.table}" }
        }
    } else {
        File("/tmp/wouldHaveSent").writeText(insertAllRequest.toString())
        log.info { "Rows (${records.count()}) ready to post but will skip due to postToBigQuery flag set to false" }
    }
}

fun JsonObject.toRowMap(fieldDefMap: MutableMap<String, FieldDef>): MutableMap<String, Any?> {
    File("/tmp/translateFieldDef").writeText(gson.toJson(fieldDefMap))
    File("/tmp/translateObject").writeText(gson.toJson(this))
    val rowMap: MutableMap<String, Any?> = mutableMapOf()
    fieldDefMap.forEach { defEntry ->
        val element = this.findBottomElement(defEntry.key)
        if (!postToBigQuery) {
            File("/tmp/translateProcess").appendText("${element.asString} -> ${defEntry.value.name} (${defEntry.value.type})\n")
        }
        rowMap[defEntry.value.name] = if (element is JsonNull) {
            null
        } else {
            when (defEntry.value.type) {
                Type.STRING -> element.asString
                Type.INTEGER -> element.asInt
                Type.DATETIME -> element.asString.subSequence(0, 23)
                Type.DATE -> element.asString
                Type.BOOL -> element.asBoolean
            }
        }
    }
    return rowMap
}

internal fun work(targetDate: LocalDate = LocalDate.now().minusDays(1)) {
    log.info { "Work session starting to fetch for ${if (fetchAllRecords) "ALL" else "$targetDate"} excluding $excludeTables - post to BQ: $postToBigQuery" }
    try {
        mapDef.keys.forEach { dataset ->
            mapDef[dataset]!!.keys.filter {
                !(excludeTables.contains(it)).also { excluding -> if (excluding) log.info { "Will skip excluded table $it" } }
            }
                .forEach { table ->
                    log.info { "Will attempt fetch and send for dataset $dataset, table $table, date ${if (fetchAllRecords) "ALL" else targetDate}" }
                    fetchAndSend(if (fetchAllRecords) null else targetDate, dataset, table)
                    hasPostedToday = true
                }
        }
    } catch (e: Exception) {
        log.error { "Failed to do work ${e.message} - has posted partially: $hasPostedToday" }
    }
    log.info { "Work session finished" }
}
