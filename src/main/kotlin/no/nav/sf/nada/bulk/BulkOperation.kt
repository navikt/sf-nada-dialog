package no.nav.sf.nada.bulk

import com.google.cloud.bigquery.TableId
import mu.KotlinLogging
import no.nav.sf.nada.Bootstrap
import no.nav.sf.nada.doSFBulkJobResultQuery
import no.nav.sf.nada.parseCSVToJsonArray
import no.nav.sf.nada.remapAndSendRecords
import java.lang.Exception
import java.lang.RuntimeException

object BulkOperation {
    private val log = KotlinLogging.logger { }

    @Volatile
    var operationIsActive: Boolean = false

    var dataset: String = ""

    var table: String = ""

    var jobId: String = ""

    @Volatile
    var jobComplete: Boolean = false

    var currentResultLocator: String = ""

    @Volatile
    var dataTransferIsActive: Boolean = false

    @Volatile
    var dataTransferReport: String = ""

    fun runTransferJob() {
        val fieldDef = Bootstrap.mapDef[dataset]!![table]!!.fieldDefMap
        val tableId = TableId.of(dataset, table)
        var locator: String? = null

        do {
            val response = doSFBulkJobResultQuery(jobId, locator)
            val array = parseCSVToJsonArray(response.bodyString())
            try {
                remapAndSendRecords(array, tableId, fieldDef)
            } catch (e: Exception) {
                dataTransferReport += "\nFail in batch operation - ${e.message}"
                throw RuntimeException("Fail in batch operation - ${e.message}")
            }
            locator = response.header("Sforce-Locator")
            val reportRow = "Processed ${array.size()} records, next locator: $locator"
            log.info { reportRow }
            dataTransferReport += "\n$reportRow"
        } while (locator != null)
    }
}
