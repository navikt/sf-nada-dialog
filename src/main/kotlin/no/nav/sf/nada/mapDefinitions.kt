package no.nav.sf.nada

import com.google.gson.JsonObject
import com.google.gson.JsonParser

data class FieldDef(val name: String, val type: SupportedType)
data class TableDef(val query: String, val fieldDefMap: MutableMap<String, FieldDef>)

fun parseMapDef(filePath: String): Map<String, Map<String, TableDef>> =
    parseMapDef(JsonParser.parseString(Application::class.java.getResource(filePath).readText()) as JsonObject)

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
