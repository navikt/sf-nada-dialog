package no.nav.sf.nada.bulk

object BulkOperation {
    @Volatile
    var operationIsActive: Boolean = false

    var dataset: String = ""

    var table: String = ""

    val jobId: Long = 0L
}
