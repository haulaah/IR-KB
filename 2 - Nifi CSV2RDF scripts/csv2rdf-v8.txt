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

        // Write RDF prefixes
        writer.write("""@prefix : <https://haulaah.github.io/CIRPO#> .
@prefix xsd: <http://www.w3.org/2001/XMLSchema#> .
@prefix rdf: <http://www.w3.org/1999/02/22-rdf-syntax-ns#> .
@prefix rdfs: <http://www.w3.org/2000/01/rdf-schema#> .
@prefix prov: <http://www.w3.org/ns/prov#> .
@prefix dct: <http://purl.org/dc/terms/> .

""")

        def parser = CSVFormat.DEFAULT.withFirstRecordAsHeader().withIgnoreEmptyLines().withTrim().parse(reader)
        def safeGet = { record, header -> record.isMapped(header) ? record.get(header)?.trim() : null }
        def escape = { text -> text ? text.replace('"', '\\"') : null }

        // Map task title to COA category
        def mapTaskTitleToCategory = { title ->
            if (!title) return 'CourseOfAction'
            def lowerTitle = title.toLowerCase()
            if (lowerTitle =~ /(detection|identification)/) return 'Detection'
            if (lowerTitle =~ /analysis/) return 'Analysis'
            if (lowerTitle =~ /(containment|isolation)/) return 'Containment'
            if (lowerTitle =~ /(eradication|remediation)/) return 'Eradication'
            if (lowerTitle =~ /recovery/) return 'Recovery'
            if (lowerTitle =~ /(post[- ]incident activity|lessons learned|review|reporting)/) return 'PostIncidentActivity'
            return 'CourseOfAction'
        }

        parser.records.each { record ->
            try {
                def incidentId = safeGet(record, 'id')
                if (!incidentId) {
                    log.warn("Skipping record with missing incident ID")
                    return
                }

                def incidentUri = "<https://haulaah.github.io/CIRPO/incident/${incidentId}>"
                writer.write("$incidentUri a :Incident ;\n")
                writer.write("    :hasIncidentId \"${incidentId}\" ;\n")

                def writeLiteral = { property, value ->
                    if (value) writer.write("    :$property \"${escape(value)}\" ;\n")
                }

                def writeDate = { property, value ->
                    if (value) {
                        try {
                            def dt = java.time.format.DateTimeFormatter.ofPattern("dd/MM/yyyy HH:mm").parse(value)
                            def isoDate = java.time.LocalDateTime.from(dt).toString()
                            writer.write("    :$property \"${isoDate}\"^^xsd:dateTime ;\n")
                        } catch (_) {
                            writer.write("    :$property \"${escape(value)}\" ;\n")
                        }
                    }
                }

                writeLiteral('hasIncidentName', safeGet(record, 'title'))
                writeLiteral('hasIncidentDescription', safeGet(record, 'description'))
                writeLiteral('hasIncidentSeverity', safeGet(record, 'severity'))
                writeDate('hasIncidentCreationDateTime', safeGet(record, 'createdAt'))
                writeDate('hasConfirmationDateTime', safeGet(record, 'startDate'))
                writeDate('hasEndDateTime', safeGet(record, 'endDate'))
                writeLiteral('hasIncidentStatus', safeGet(record, 'status'))
                writeLiteral('hasIncidentStage', safeGet(record, 'stage'))

                def analystEmail = safeGet(record, 'createdBy')
                if (analystEmail) {
                    writer.write("    :hasConfirmationAnalyst [ a :Analyst ; :hasAnalystEmail \"${escape(analystEmail)}\" ] ;\n")
                }

                // Prepare COA category counters for unique identifiers
                def coaCategoryCounters = [:].withDefault { 0 } 

                // Process Tasks (COAs)
                def coaLinks = []
                def tasksJson = safeGet(record, 'tasks')
                if (tasksJson) {
                    try {
                        def tasks = jsonSlurper.parseText(tasksJson)
                        tasks.each { task ->
                            def taskTitle = task['title'] ?: 'Unknown'
                            def coaCategory = mapTaskTitleToCategory(taskTitle)
                            coaCategoryCounters[coaCategory]++
                            def count = coaCategoryCounters[coaCategory]
                            def taskUri = "<https://haulaah.github.io/CIRPO/courseofaction/${incidentId}_${coaCategory.toLowerCase()}_${count}>"
                            coaLinks << taskUri
                        }
                    } catch (ex) {
                        log.error("Failed to parse tasks JSON: ${ex.message}")
                    }
                }

                // Process Observables as Artefacts
                def observableLinks = []
                def observablesJson = safeGet(record, 'observables')
                if (observablesJson) {
                    try {
                        def observables = jsonSlurper.parseText(observablesJson)
                        def artefactCounter = 1
                        observables.each { obs ->
                            def obsUri = "<https://haulaah.github.io/CIRPO/artefact/${incidentId}_artefact_${artefactCounter}>"
                            artefactCounter++
                            observableLinks << obsUri
                        }
                    } catch (ex) {
                        log.error("Failed to parse artefacts JSON: ${ex.message}")
                    }
                }

                // Process Security Events and EventActions
                def eventActionLinks = []
                def eventsJson = safeGet(record, 'Event')
                def eventUri = null
                def events = []
                if (eventsJson) {
                    try {
                        events = jsonSlurper.parseText(eventsJson)
                        if (events && events.size() > 0) {
                            def evtId = events[0]['event_id'] ?: "event_${incidentId}"
                            eventUri = "<https://haulaah.github.io/CIRPO/securityevent/${incidentId}_${evtId}>"
                            events.eachWithIndex { evtAction, idx ->
                                def actionUri = "<https://haulaah.github.io/CIRPO/securityevent/${incidentId}_${evtId}_action_${idx + 1}>"
                                eventActionLinks << actionUri
                            }
                        }
                    } catch (ex) {
                        log.error("Failed to parse security events JSON: ${ex.message}")
                    }
                }

                // Process Analyses
                def analysisLinks = []
                def analysisJson = safeGet(record, 'Observables Analysis')
                if (analysisJson) {
                    try {
                        def analysisList = jsonSlurper.parseText(analysisJson)
                        def analysisCounter = 1
                        analysisList.each {
                            def analysisUri = "<https://haulaah.github.io/CIRPO/securityanalysis/${incidentId}_analysis_${analysisCounter}>"
                            analysisCounter++
                            analysisLinks << analysisUri
                        }
                    } catch (ex) {
                        log.error("Failed to parse security analysis JSON: ${ex.message}")
                    }
                }

                // Write links from Incident to COAs, Observables, Events, Analysis
                coaLinks.each { writer.write("    :hasCoa ${it} ;\n") }
                observableLinks.each { writer.write("    :hasArtefact ${it} ;\n") }
                if (eventUri) {
                    eventActionLinks.each { writer.write("    :hasSecurityEventAction ${it} ;\n") }
                }
                analysisLinks.each { writer.write("    :hasSecurityAnalysis ${it} ;\n") }

                writer.write(".\n\n") // close Incident

                // Write COA details with unique URIs
                if (tasksJson) {
                    def tasks = jsonSlurper.parseText(tasksJson)
                    // Reset counters for writing detailed COA RDF
                    coaCategoryCounters = [:].withDefault { 0 } 
                    tasks.each { task ->
                        def taskTitle = task['title'] ?: 'Unknown'
                        def coaCategory = mapTaskTitleToCategory(taskTitle)
                        coaCategoryCounters[coaCategory]++
                        def count = coaCategoryCounters[coaCategory]
                        def taskUri = "<https://haulaah.github.io/CIRPO/courseofaction/${incidentId}_${coaCategory.toLowerCase()}_${count}>"

                        writer.write("${taskUri} a :CourseOfAction ;\n")
                        writer.write("    a :${coaCategory} ;\n")
                        writeLiteral('hasCoaDescription', task['description'])
                        writeLiteral('hasCoaStatus', task['status'])
                        writeDate('hasCoaCreationDateTime', task['createdAt'])
                        writeDate('hasCoaStartDateTime', task['startDate'])
                        writeDate('hasCoaEndDateTime', task['endDate'])
                        if (task['createdBy']) writer.write("    :hasCoaCreationAnalyst [ a :Analyst ; :hasAnalystEmail \"${escape(task['createdBy'])}\" ] ;\n")
                        if (task['owner']) writer.write("    :hasCoaAssignedAnalyst [ a :Analyst ; :hasAnalystEmail \"${escape(task['owner'])}\" ] ;\n")
                        writer.write(".\n\n")
                    }
                }

                // Write Artefacts
                if (observablesJson) {
                    def artefactCounter = 1
                    def observables = jsonSlurper.parseText(observablesJson)
                    observables.each { obs ->
                        def obsUri = "<https://haulaah.github.io/CIRPO/artefact/${incidentId}_artefact_${artefactCounter}>"
                        artefactCounter++
                        writer.write("${obsUri} a :Artefact ;\n")
                        writeLiteral('hasArtefactType', obs['dataType'])
                        writeLiteral('hasArtefactDataValue', obs['data'])
                        if (obs['createdBy']) writer.write("    :hasArtefactCreator [ a :Analyst ; :hasAnalystEmail \"${escape(obs['createdBy'])}\" ] ;\n")
                        writeDate('hasArtefactCreationDateTime', obs['createdAt'])
                        writer.write(".\n\n")
                    }
                }

                // Write SecurityEvents and Actions
                if (events && events.size() > 0) {
                    def evtId = events[0]['event_id'] ?: "event_${incidentId}"
                    def eventUriStr = "<http://irkb.com/event/${incidentId}_${evtId}>"
                    writer.write("${eventUriStr} a :SecurityEvent ;\n")
                    def eventTitle = events[0]['title'] ?: null
                    if (eventTitle) writer.write("    :hasEventTitle \"${escape(eventTitle)}\" ;\n")
                    writer.write("    :hasSecurityEventId \"${escape(evtId)}\" ;\n")
                    events.eachWithIndex { evtAction, idx ->
                        def actionUri = "<https://haulaah.github.io/CIRPO/securityevent/${incidentId}_${evtId}_action_${idx + 1}>"
                        writer.write("    :hasSecurityEventAction ${actionUri} ;\n")
                    }
                    writer.write(".\n\n")

                    events.eachWithIndex { evtAction, idx ->
                        def actionUri = "<https://haulaah.github.io/CIRPO/securityevent/${incidentId}_${evtId}_action_${idx + 1}>"
                        writer.write("${actionUri} a :SecurityEventAction ;\n")
                        writeLiteral('hasSecurityEventActivity', evtAction['action'])
                        writeLiteral('hasSecurityEventDescription', evtAction['title'])
                        if (evtAction['user_email']) writer.write("    prov:wasPerformedBy [ a :Analyst ; :hasAnalystEmail \"${escape(evtAction['user_email'])}\" ] ;\n")
                        writeDate('hasSecurityEventActivityDateTime', evtAction['created'])
                        writer.write(".\n\n")
                    }
                }

                // Write SecurityAnalyses
                if (analysisJson) {
                    def analysisCounter = 1
                    def analysisList = jsonSlurper.parseText(analysisJson)
                    analysisList.each { analysis ->
                        def analysisUri = "<https://haulaah.github.io/CIRPO/securityanalysis/${incidentId}_analysis_${analysisCounter}>"
                        analysisCounter++
                        writer.write("${analysisUri} a :SecurityAnalysis ;\n")
                        writeLiteral('hasSecurityAnalysisDataType', analysis['dataType'])
                        writeLiteral('hasSecurityAnalysisData', analysis['data'])
                        writeLiteral('hasSecurityAnalysisReport', analysis['report'])
                        if (analysis['createdBy']) writer.write("    :hasSecurityAnalysisPerformer [ a :Analyst ; :hasAnalystEmail \"${escape(analysis['createdBy'])}\" ] ;\n")
                        writeDate('hasSecurityAnalysisDateTime', analysis['createdAt'])
                        writer.write(".\n\n")
                    }
                }

            } catch (recordEx) {
                log.error("Error processing record: ${recordEx.message}")
            }
        }

        writer.flush()
    } as StreamCallback)

    session.transfer(flowFile, REL_SUCCESS)
} catch (ex) {
    log.error("Processing failed: ${ex.message}")
    session.transfer(flowFile, REL_FAILURE)
}
