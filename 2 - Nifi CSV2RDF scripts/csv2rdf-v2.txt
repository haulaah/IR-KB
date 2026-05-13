import org.apache.commons.csv.*
import java.nio.charset.StandardCharsets
import groovy.json.JsonSlurper

def flowFile = session.get()
if (!flowFile) return

try {
    flowFile = session.write(flowFile, { inputStream, outputStream ->
        def reader = new InputStreamReader(inputStream, StandardCharsets.UTF_8)
        def writer = new OutputStreamWriter(outputStream, StandardCharsets.UTF_8)
        def jsonSlurper = new JsonSlurper()

        writer.write("""@prefix : <http://irkb.com/ir-ontology#> .
@prefix xsd: <http://www.w3.org/2001/XMLSchema#> .
@prefix rdf: <http://www.w3.org/1999/02/22-rdf-syntax-ns#> .
@prefix rdfs: <http://www.w3.org/2000/01/rdf-schema#> .
@prefix prov: <http://www.w3.org/ns/prov#> .
@prefix dct: <http://purl.org/dc/terms/> .

""")

        def parser = CSVFormat.DEFAULT
            .withFirstRecordAsHeader()
            .withIgnoreEmptyLines()
            .withTrim()
            .parse(reader)

        parser.records.each { record ->
            try {
                def safeGet = { String header ->
                    record.isMapped(header) ? record.get(header) : null
                }

                def incidentId = safeGet('id')
                if (!incidentId) {
                    log.warn("Skipping record with missing 'id'")
                    return
                }

                def incidentUri = "<http://irkb.com/incident/$incidentId>"
                writer.write("$incidentUri a :Incident ;\n")
                writer.write("    :hasIncidentId \"$incidentId\" ;\n")

                def writeLiteral = { property, value ->
                    if (value) writer.write("    :$property \"${value.replace('\"', '\\\"')}\" ;\n")
                }

                def writeDate = { property, value ->
                    if (value) writer.write("    :$property \"$value\"^^xsd:dateTime ;\n")
                }

                writeLiteral('hasIncidentName', safeGet('title'))
                writeLiteral('hasIncidentDescription', safeGet('description'))
                writeLiteral('hasIncidentSeverity', safeGet('severity'))
                writeDate('hasIncidentCreationDateTime', safeGet('createdAt'))
                writeDate('hasConfirmationDateTime', safeGet('startDate'))
                writeDate('hasEndDateTime', safeGet('endDate'))
                writeLiteral('hasIncidentStatus', safeGet('status'))
                writeLiteral('hasIncidentStage', safeGet('stage'))

                def analystEmail = safeGet('createdBy')
                if (analystEmail) {
                    writer.write("    :hasConfirmationAnalyst [ a :Analyst ; :hasAnalystEmail \"$analystEmail\" ] ;\n")
                }

                // ---- Tasks (Courses of Action) ----
                def tasksJson = safeGet('tasks')
                if (tasksJson) {
                    try {
                        def tasks = jsonSlurper.parseText(tasksJson)
                        tasks.each { task ->
                            def coaTitle = task['title'] ?: 'Unknown'
                            def coaUri = "<http://irkb.com/coa/${incidentId}_${coaTitle.replaceAll('[^a-zA-Z0-9]', '_')}>"
                            writer.write("    :hasCoa $coaUri ;\n")
                            writer.write("$coaUri a :CourseOfAction, :${coaTitle.replaceAll('[^a-zA-Z0-9]', '')} ;\n")
                            writeLiteral('hasCoaDescription', task['description'])
                            writeLiteral('hasCoaStatus', task['status'])
                            writeDate('hasCoaCreationDateTime', task['createdAt'])
                            writeDate('hasCoaStartDateTime', task['startDate'])
                            writeDate('hasCoaEndDateTime', task['endDate'])

                            def creator = task['createdBy']
                            if (creator) writer.write("    :hasCoaCreationAnalyst [ a :Analyst ; :hasAnalystEmail \"$creator\" ] ;\n")

                            def owner = task['owner']
                            if (owner) writer.write("    :hasCoaAssignedAnalyst [ a :Analyst ; :hasAnalystEmail \"$owner\" ] .\n\n")
                        }
                    } catch (e) {
                        log.error("Failed to parse tasks JSON: ${e.message}")
                    }
                }

                // ---- Observables (Artefacts) ----
                def observablesJson = safeGet('observables')
                if (observablesJson) {
                    try {
                        def observables = jsonSlurper.parseText(observablesJson)
                        observables.each { obs ->
                            def artefactType = obs['dataType'] ?: 'Unknown'
                            def artefactUri = "<http://irkb.com/artefact/${incidentId}_${artefactType.replaceAll('[^a-zA-Z0-9]', '_')}>"
                            writer.write("    :hasArtefact $artefactUri ;\n")
                            writer.write("$artefactUri a :Artefact ;\n")
                            writeLiteral('hasArtefactType', artefactType)
                            writeLiteral('hasArtefactDataValue', obs['data'])
                            writeDate('hasArtefactCreationDateTime', obs['createdAt'])

                            def creator = obs['createdBy']
                            if (creator) writer.write("    :hasArtefactCreationAnalyst [ a :Analyst ; :hasAnalystEmail \"$creator\" ] .\n\n")
                        }
                    } catch (e) {
                        log.error("Failed to parse observables JSON: ${e.message}")
                    }
                }

                // ---- Security Events ----
                def eventsJson = safeGet('Event')
                if (eventsJson) {
                    try {
                        def events = jsonSlurper.parseText(eventsJson)
                        events.each { event ->
                            def eventId = event['event_id']
                            if (!eventId) return
                            def eventUri = "<http://irkb.com/event/${eventId}>"
                            writer.write("    :hasEvent $eventUri ;\n")
                            writer.write("$eventUri a :SecurityEvent ;\n")
                            writeLiteral('hasEventId', eventId)
                            writeLiteral('hasEventDescription', event['title'])
                            writeLiteral('hasEventActivity', event['action'])
                            writeDate('hasEventActivityDateTime', event['created'])

                            def analystId = event['user_id']
                            def analystEmailEvent = event['user_email']
                            writer.write("    prov:wasPerformedBy [ a :Analyst ; ")
                            if (analystId) writer.write(":hasAnalystId \"$analystId\" ; ")
                            if (analystEmailEvent) writer.write(":hasAnalystEmail \"$analystEmailEvent\" ")
                            writer.write("] .\n\n")
                        }
                    } catch (e) {
                        log.error("Failed to parse events JSON: ${e.message}")
                    }
                }

                // ---- Observables Analysis (Security Analysis) ----
                def analysisJson = safeGet('Observables Analysis')
                if (analysisJson) {
                    try {
                        def analyses = jsonSlurper.parseText(analysisJson)
                        analyses.each { analysis ->
                            def analysisType = analysis['dataType'] ?: 'Unknown'
                            def analysisUri = "<http://irkb.com/analysis/${incidentId}_${analysisType.replaceAll('[^a-zA-Z0-9]', '_')}>"
                            writer.write("    prov:wasGeneratedBy $analysisUri ;\n")
                            writer.write("$analysisUri a :SecurityAnalysis ;\n")
                            writeLiteral('hasSecurityAnalysisType', analysisType)
                            writeLiteral('hasSecurityAnalysisDataValue', analysis['data'])
                            writeDate('hasSecurityAnalysisDateTime', analysis['createdAt'])
                            writeLiteral('hasSecurityAnalysisDetails', analysis['report'])

                            def creator = analysis['createdBy']
                            if (creator) writer.write("    prov:wasPerformedBy [ a :Analyst ; :hasAnalystEmail \"$creator\" ] .\n\n")
                        }
                    } catch (e) {
                        log.error("Failed to parse analysis JSON: ${e.message}")
                    }
                }

                writer.write(".\n\n")
            } catch (recordError) {
                log.error("Error processing record: ${recordError.message}", recordError)
            }
        }

        writer.flush()
    } as StreamCallback)

    session.transfer(flowFile, REL_SUCCESS)
} catch (Exception e) {
    log.error("Processing failed: ${e.message}", e)
    session.transfer(flowFile, REL_FAILURE)
}
