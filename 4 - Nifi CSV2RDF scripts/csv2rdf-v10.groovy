//CSV2RDF-CIRPO-IRMRKB


import org.apache.commons.csv.*
import java.nio.charset.StandardCharsets
import groovy.json.JsonSlurper
import groovy.json.JsonParserType

def flowFile = session.get()
if (!flowFile) return

try {
    flowFile = session.write(flowFile, { inputStream, outputStream ->
        def reader = new InputStreamReader(inputStream, StandardCharsets.UTF_8)
        def writer = new OutputStreamWriter(outputStream, StandardCharsets.UTF_8)
        def jsonSlurper = new JsonSlurper().setType(JsonParserType.LAX)

        writer.write("""@prefix : <https://haulaah.github.io/CIRPO#> .
@prefix xsd: <http://www.w3.org/2001/XMLSchema#> .
@prefix rdf: <http://www.w3.org/1999/02/22-rdf-syntax-ns#> .
@prefix rdfs: <http://www.w3.org/2000/01/rdf-schema#> .
@prefix prov: <http://www.w3.org/ns/prov#> .
@prefix dct: <http://purl.org/dc/terms/> .

""")

        def parser = CSVFormat.DEFAULT.withFirstRecordAsHeader().withIgnoreEmptyLines().withTrim().parse(reader)
        def safeGet = { record, header -> record.isMapped(header) ? record.get(header)?.trim() : null }
        def escape = { text -> text ? text.replace('"', '\\"').replace('\n', ' ').replace('\r', ' ') : null }


        def cleanJsonString = { String raw ->
            if (!raw) return raw
            def validEscapes = ['\\n', '\\r', '\\t', '\\"', '\\\\']
            def placeholders = ['##NEWLINE##', '##RETURN##', '##TAB##', '##DQUOTE##', '##BACKSLASH##']
            def temp = raw
            for (int i = 0; i < validEscapes.size(); i++) {
                temp = temp.replace(validEscapes[i], placeholders[i])
            }
            temp = temp.replace('\\', '\\\\')
            for (int i = 0; i < validEscapes.size(); i++) {
                temp = temp.replace(placeholders[i], validEscapes[i])
            }
            temp = temp.replace('\n', '\\n').replace('\r', '\\r')
            return temp
        }

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
            def incidentId = safeGet(record, 'id')
            if (!incidentId) {
                log.warn("Skipping record with missing incident ID")
                return
            }

            try {
                def incidentUri = "<https://haulaah.github.io/CIRPO/incident/${incidentId}>"
                writer.write("$incidentUri a :Incident ;\n")
                writer.write("    :hasIncidentId \"${incidentId}\" ;\n")

                def writeLiteral = { property, value ->
                    if (value) writer.write("    :$property \"${escape(value)}\" ;\n")
                }

                def writeDate = { property, value ->
                    if (!value) return
                    def isoDate = null
                    def patterns = [
                        "dd/MM/yyyy HH:mm",
                        "yyyy-MM-dd HH:mm:ss",
                        "yyyy-MM-dd HH:mm"
                    ]
                    for (pattern in patterns) {
                        try {
                            def dt = java.time.format.DateTimeFormatter.ofPattern(pattern).parse(value)
                            def localDateTime = java.time.LocalDateTime.from(dt)
                            isoDate = localDateTime.toString()
                            break
                        } catch (Exception ignored) {}
                    }
                    if (isoDate) {
                        writer.write("    :$property \"${isoDate}\"^^xsd:dateTime ;\n")
                    } else {
                        log.warn("Incident ${incidentId}: Could not parse date for ${property} = ${value}")
                        writer.write("    :$property \"${escape(value)}\" ;\n")
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

                // --- Case Tasks ---
                def coaLinks = []
                def tasks = []
                def tasksJsonRaw = safeGet(record, 'tasks')
                if (tasksJsonRaw) {
                    try {
                        def cleaned = cleanJsonString(tasksJsonRaw)
                        tasks = jsonSlurper.parseText(cleaned)
                        def coaCategoryCounters = [:].withDefault { 0 }
                        tasks.each { task ->
                            def taskTitle = task['title'] ?: 'Unknown'
                            def coaCategory = mapTaskTitleToCategory(taskTitle)
                            coaCategoryCounters[coaCategory]++
                            def count = coaCategoryCounters[coaCategory]
                            def taskUri = "<https://haulaah.github.io/CIRPO/courseofaction/${incidentId}_${coaCategory.toLowerCase()}_${count}>"
                            coaLinks << taskUri
                        }
                    } catch (ex) {
                        log.error("Incident ${incidentId}: Failed to parse tasks JSON. Error: ${ex.message}")
                    }
                }

                // --- Observables / Artefacts ---
                def observableLinks = []
                def observables = []
                def observablesJsonRaw = safeGet(record, 'observables')
                if (observablesJsonRaw) {
                    try {
                        def cleaned = cleanJsonString(observablesJsonRaw)
                        observables = jsonSlurper.parseText(cleaned)
                        def artefactCounter = 1
                        observables.each { obs ->
                            def obsUri = "<https://haulaah.github.io/CIRPO/artefact/${incidentId}_artefact_${artefactCounter}>"
                            artefactCounter++
                            observableLinks << obsUri
                        }
                    } catch (ex) {
                        log.error("Incident ${incidentId}: Failed to parse observables JSON. Error: ${ex.message}")
                    }
                }

                // --- Event / SecurityEvent ---
                def eventActionLinks = []
                def events = []
                def eventUri = null
                def eventsJsonRaw = safeGet(record, 'Event')
                if (eventsJsonRaw) {
                    try {
                        def cleaned = cleanJsonString(eventsJsonRaw)
                        events = jsonSlurper.parseText(cleaned)
                        if (events && events.size() > 0) {
                            def evtId = events[0]['event_id'] ?: "event_${incidentId}"
                            eventUri = "<https://haulaah.github.io/CIRPO/securityevent/${incidentId}_${evtId}>"
                            events.eachWithIndex { evtAction, idx ->
                                def actionUri = "<https://haulaah.github.io/CIRPO/securityeventaction/${incidentId}_${evtId}_action_${idx + 1}>"
                                eventActionLinks << actionUri
                            }
                        }
                    } catch (ex) {
                        log.error("Incident ${incidentId}: Failed to parse Event JSON. Error: ${ex.message}")
                    }
                }

                // --- Observables Analysis / Security Analysis ---
                def analysisLinks = []
                def analysisList = []
                def analysisJsonRaw = safeGet(record, 'Observables Analysis')
                if (analysisJsonRaw) {
                    try {
                        def cleaned = cleanJsonString(analysisJsonRaw)
                        analysisList = jsonSlurper.parseText(cleaned)
                        def analysisCounter = 1
                        analysisList.each {
                            def analysisUri = "<https://haulaah.github.io/CIRPO/securityanalysis/${incidentId}_analysis_${analysisCounter}>"
                            analysisCounter++
                            analysisLinks << analysisUri
                        }
                    } catch (ex) {
                        log.error("Incident ${incidentId}: Failed to parse Observables Analysis JSON. Error: ${ex.message}")
                    }
                }

                // --- ResponderChat / Responder Communication ---
                def chatChannelUri = null
                def chatMessageLinks = []
                def chatMessages = []
                def chatJsonRaw = safeGet(record, 'ResponderChat')
                if (chatJsonRaw) {
                    try {
                        def cleaned = cleanJsonString(chatJsonRaw)
                        chatMessages = jsonSlurper.parseText(cleaned)
                        if (chatMessages && chatMessages.size() > 0) {
                            def channelId = chatMessages[0]['channel_id'] ?: "channel_${incidentId}"
                            chatChannelUri = "<https://haulaah.github.io/CIRPO/chatchannel/${incidentId}_${channelId}>"
                            chatMessages.eachWithIndex { msg, idx ->
                                def msgUri = "<https://haulaah.github.io/CIRPO/chatmessage/${incidentId}_msg_${idx + 1}>"
                                chatMessageLinks << msgUri
                            }
                        }
                    } catch (ex) {
                        log.error("Incident ${incidentId}: Failed to parse ResponderChat JSON. Error: ${ex.message}")
                    }
                }

              
                coaLinks.each { writer.write("    :hasCoa ${it} ;\n") }
                observableLinks.each { writer.write("    :hasArtefact ${it} ;\n") }
                if (eventUri) {
                    // FIX: use :hasEvent (range SecurityEvent) for SecurityEventAction (subclass of SecurityEvent)
                    eventActionLinks.each { writer.write("    :hasEvent ${it} ;\n") }
                }
                
                analysisLinks.each { writer.write("    :hasAnalysis ${it} ;\n") }
                if (chatChannelUri) {
                    writer.write("    :hasChannel ${chatChannelUri} ;\n")
                }
                writer.write(".\n\n")

                // --- Write COAs ---
                if (tasks) {
                    def coaCategoryCounters = [:].withDefault { 0 }
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

                // --- Write Artefacts ---
                if (observables) {
                    def artefactCounter = 1
                    observables.each { obs ->
                        def obsUri = "<https://haulaah.github.io/CIRPO/artefact/${incidentId}_artefact_${artefactCounter}>"
                        artefactCounter++
                        writer.write("${obsUri} a :Artefact ;\n")
                        writeLiteral('hasArtefactType', obs['dataType'])
                        writeLiteral('hasArtefactDataValue', obs['data'])
                        if (obs['createdBy']) writer.write("    :hasArtefactCreationAnalyst [ a :Analyst ; :hasAnalystEmail \"${escape(obs['createdBy'])}\" ] ;\n")
                        writeDate('hasArtefactCreationDateTime', obs['createdAt'])
                        writer.write(".\n\n")
                    }
                }

                // --- Write SecurityEvent and SecurityEventAction ---
                if (events && events.size() > 0) {
                    def evtId = events[0]['event_id'] ?: "event_${incidentId}"
                    def eventUriStr = "<https://haulaah.github.io/CIRPO/securityevent/${incidentId}_${evtId}>"
                    writer.write("${eventUriStr} a :SecurityEvent ;\n")
                    def eventTitle = events[0]['title'] ?: null
                    if (eventTitle) writer.write("    :hasEventTitle \"${escape(eventTitle)}\" ;\n")
                    writer.write("    :hasEventId \"${escape(evtId)}\" ;\n")
                    events.eachWithIndex { evtAction, idx ->
                        def actionUri = "<https://haulaah.github.io/CIRPO/securityeventaction/${incidentId}_${evtId}_action_${idx + 1}>"
                        writer.write("    :hasEvent ${actionUri} ;\n")
                    }
                    writer.write(".\n\n")

                    events.eachWithIndex { evtAction, idx ->
                        def actionUri = "<https://haulaah.github.io/CIRPO/securityeventaction/${incidentId}_${evtId}_action_${idx + 1}>"
                        writer.write("${actionUri} a :SecurityEventAction ;\n")
                        writeLiteral('hasEventActivity', evtAction['action'])
                        writeLiteral('hasEventDescription', evtAction['title'])
                        if (evtAction['user_email']) writer.write("    prov:wasPerformedBy [ a :Analyst ; :hasAnalystEmail \"${escape(evtAction['user_email'])}\" ] ;\n")
                        writeDate('hasEventActivityDateTime', evtAction['created'])
                        writer.write(".\n\n")
                    }
                }

                // --- Write SecurityAnalysis ---
                if (analysisList) {
                    def analysisCounter = 1
                    analysisList.each { analysis ->
                        def analysisUri = "<https://haulaah.github.io/CIRPO/securityanalysis/${incidentId}_analysis_${analysisCounter}>"
                        analysisCounter++
                        writer.write("${analysisUri} a :SecurityAnalysis ;\n")
                        writeLiteral('hasSecurityAnalysisType', analysis['dataType'])
                        writeLiteral('hasSecurityAnalysisDataValue', analysis['data'])
                        writeLiteral('hasSecurityAnalysisDetails', analysis['report'])
                        if (analysis['createdBy']) writer.write("    :hasAnalysisPerformingAnalyst [ a :Analyst ; :hasAnalystEmail \"${escape(analysis['createdBy'])}\" ] ;\n")
                        writeDate('hasSecurityAnalysisDateTime', analysis['createdAt'])
                        writer.write(".\n\n")
                    }
                }

                // --- Write ChatChannel and ChatMessage (with ontology alignment) ---
                if (chatMessages && chatMessages.size() > 0) {
                    def channelId = chatMessages[0]['channel_id'] ?: "channel_${incidentId}"
                    def channelName = chatMessages[0]['channel_name'] ?: "incident-channel"
                    def channelUri = "<https://haulaah.github.io/CIRPO/chatchannel/${incidentId}_${channelId}>"
                    writer.write("${channelUri} a :ChatChannel ;\n")
                    writer.write("    :hasChatChannelId \"${escape(channelId)}\" ;\n")
                    writer.write("    :hasChatChannelName \"${escape(channelName)}\" ;\n")
                    writer.write("    :isRelatedToIncident ${incidentUri} ;\n")
                    chatMessages.eachWithIndex { msg, idx ->
                        def msgUri = "<https://haulaah.github.io/CIRPO/chatmessage/${incidentId}_msg_${idx + 1}>"
                        writer.write("    :containsChat ${msgUri} ;\n")
                    }
                    writer.write(".\n\n")

                    chatMessages.eachWithIndex { msg, idx ->
                        def msgUri = "<https://haulaah.github.io/CIRPO/chatmessage/${incidentId}_msg_${idx + 1}>"
                        writer.write("${msgUri} a :ChatMessage ;\n")
                        writer.write("    :hasChatMessageId \"${escape(msg['post_id'])}\" ;\n")
                        def message = msg['message'] ?: ''
                        writer.write("    :hasChatMessage \"${escape(message)}\" ;\n")
                        writeDate('hasChatMessageDateTime', msg['created_at'])
                        writer.write("    :wasPostedTo ${channelUri} ;\n")
                        writer.write("    :isRelatedToIncident ${incidentUri} ;\n")
                        
                        def userObj = msg['user']
                        if (userObj) {
                            writer.write("    :hasChatAnalyst [ a :Analyst ;")
                            if (userObj['id']) writer.write(" :hasAnalystId \"${escape(userObj['id'])}\" ;")
                            if (userObj['email']) writer.write(" :hasAnalystEmail \"${escape(userObj['email'])}\" ;")
                            writer.write(" ] ;\n")
                        } else {
                            
                            if (msg['user_id'] || msg['user_email']) {
                                writer.write("    :hasChatAnalyst [ a :Analyst ;")
                                if (msg['user_id']) writer.write(" :hasAnalystId \"${escape(msg['user_id'])}\" ;")
                                if (msg['user_email']) writer.write(" :hasAnalystEmail \"${escape(msg['user_email'])}\" ;")
                                writer.write(" ] ;\n")
                            }
                        }
                        writer.write(".\n\n")
                    }
                }

            } catch (recordEx) {
                log.error("Error processing incident ${incidentId}: ${recordEx.message}", recordEx)
            }
        }

        writer.flush()
    } as StreamCallback)

    session.transfer(flowFile, REL_SUCCESS)
} catch (ex) {
    log.error("Processing failed: ${ex.message}", ex)
    session.transfer(flowFile, REL_FAILURE)
}
