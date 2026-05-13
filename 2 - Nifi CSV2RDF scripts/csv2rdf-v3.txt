import org.apache.commons.csv.*
import java.nio.charset.StandardCharsets
import groovy.json.JsonSlurper

// Get the flow file
def flowFile = session.get()
if (!flowFile) return

try {
    // Process the CSV and generate RDF
    flowFile = session.write(flowFile, { inputStream, outputStream -> 
        def reader = new InputStreamReader(inputStream, StandardCharsets.UTF_8)
        def writer = new OutputStreamWriter(outputStream, StandardCharsets.UTF_8)
        def jsonSlurper = new groovy.json.JsonSlurper()
        
        // Write RDF header with IR ontology prefixes
        writer.write("""@prefix : <http://irkb.com/ir-ontology#> .
@prefix xsd: <http://www.w3.org/2001/XMLSchema#> .
@prefix rdf: <http://www.w3.org/1999/02/22-rdf-syntax-ns#> .
@prefix rdfs: <http://www.w3.org/2000/01/rdf-schema#> .
@prefix prov: <http://www.w3.org/ns/prov#> .
@prefix dct: <http://purl.org/dc/terms/> .

""")

        // Parse CSV with header
        def parser = CSVFormat.DEFAULT
            .withFirstRecordAsHeader()
            .withIgnoreEmptyLines()
            .withTrim()
            .parse(reader)

        // Process each record
        parser.records.each { record ->
            try {
                // Create Incident URI
                def incidentId = record.get('id')
                def incidentUri = "<http://irkb.com/incident/$incidentId>"
                
                // Write Incident triples
                writer.write("$incidentUri a :Incident ;\n")
                writer.write("    :hasIncidentId \"$incidentId\" ;\n")
                writer.write("    :hasIncidentName \"${record.get('title').replace('"', '\\"')}\" ;\n")
                writer.write("    :hasIncidentDescription \"${record.get('description').replace('"', '\\"')}\" ;\n")
                writer.write("    :hasIncidentSeverity \"${record.get('severity')}\" ;\n")
                
                // Date format conversion (dd/MM/yyyy to yyyy-MM-dd)
                def convertDate = { dateStr ->
                    if (!dateStr) return null
                    def parts = dateStr.split(' ')[0].split('/')
                    "${parts[2]}-${parts[1]}-${parts[0]}${dateStr.contains(' ') ? 'T' + dateStr.split(' ')[1] + ':00' : ''}"
                }
                
                writer.write("    :hasIncidentCreationDateTime \"${convertDate(record.get('createdAt'))}\"^^xsd:dateTime ;\n")
                writer.write("    :hasConfirmationDateTime \"${convertDate(record.get('startDate'))}\"^^xsd:dateTime ;\n")
                writer.write("    :hasEndDateTime \"${convertDate(record.get('endDate'))}\"^^xsd:dateTime ;\n")
                writer.write("    :hasIncidentStatus \"${record.get('status')}\" ;\n")
                writer.write("    :hasIncidentStage \"${record.get('stage')}\" ;\n")
                
                // Add confirmation analyst
                def analystEmail = record.get('createdBy')
                writer.write("    :hasConfirmationAnalyst [ a :Analyst ; :hasAnalystEmail \"$analystEmail\" ] ;\n")
                
                // Process Tasks (Courses of Action)
                def tasksJson = record.get('tasks')
                if (tasksJson && tasksJson != '[]') {
                    try {
                        def tasks = jsonSlurper.parseText(tasksJson)
                        tasks.each { task ->
                            def coaUri = "<http://irkb.com/coa/${incidentId}_${task['title'].replaceAll('[^a-zA-Z0-9]','_')}>"
                            writer.write("    :hasCoa $coaUri ;\n")
                            writer.write("$coaUri a :CourseOfAction, :${task['title'].replace(' ','')} ;\n")
                            writer.write("    :hasCoaDescription \"${task['description'].replace('"', '\\"')}\" ;\n")
                            writer.write("    :hasCoaStatus \"${task['status']}\" ;\n")
                            writer.write("    :hasCoaCreationDateTime \"${task['createdAt'].replace(' ','T')}\"^^xsd:dateTime ;\n")
                            writer.write("    :hasCoaStartDateTime \"${task['startDate'].replace(' ','T')}\"^^xsd:dateTime ;\n")
                            writer.write("    :hasCoaEndDateTime \"${task['endDate'].replace(' ','T')}\"^^xsd:dateTime ;\n")
                            writer.write("    :hasCoaCreationAnalyst [ a :Analyst ; :hasAnalystEmail \"${task['createdBy']}\" ] ;\n")
                            writer.write("    :hasCoaAssignedAnalyst [ a :Analyst ; :hasAnalystEmail \"${task['owner']}\" ] .\n\n")
                        }
                    } catch (e) {
                        log.error("Failed to parse tasks JSON: ${e.message}")
                    }
                }
                
                // Process Observables (Artefacts)
                def observablesJson = record.get('observables')
                if (observablesJson && observablesJson != '[]') {
                    try {
                        def observables = jsonSlurper.parseText(observablesJson)
                        observables.each { obs ->
                            def artefactUri = "<http://irkb.com/artefact/${incidentId}_${obs['dataType']}>"
                            writer.write("    :hasArtefact $artefactUri ;\n")
                            writer.write("$artefactUri a :Artefact ;\n")
                            writer.write("    :hasArtefactType \"${obs['dataType']}\" ;\n")
                            writer.write("    :hasArtefactDataValue \"${obs['data']}\" ;\n")
                            writer.write("    :hasArtefactCreationDateTime \"${obs['createdAt'].replace(' ','T')}\"^^xsd:dateTime ;\n")
                            writer.write("    :hasArtefactCreationAnalyst [ a :Analyst ; :hasAnalystEmail \"${obs['createdBy']}\" ] .\n\n")
                        }
                    } catch (e) {
                        log.error("Failed to parse observables JSON: ${e.message}")
                    }
                }
                
                // Process Security Events
                def eventsJson = record.get('Event')
                if (eventsJson && eventsJson != '[]') {
                    try {
                        def events = jsonSlurper.parseText(eventsJson)
                        events.each { event ->
                            def eventUri = "<http://irkb.com/event/${event['event_id']}>"
                            writer.write("    :hasEvent $eventUri ;\n")
                            writer.write("$eventUri a :SecurityEvent ;\n")
                            writer.write("    :hasEventId \"${event['event_id']}\" ;\n")
                            writer.write("    :hasEventDescription \"${event['title'].replace('"', '\\"')}\" ;\n")
                            writer.write("    :hasEventActivity \"${event['action']}\" ;\n")
                            writer.write("    :hasEventActivityDateTime \"${convertDate(event['created'])}\"^^xsd:dateTime ;\n")
                            writer.write("    prov:wasPerformedBy [ a :Analyst ; :hasAnalystId \"${event['user_id']}\" ; :hasAnalystEmail \"${event['user_email']}\" ] .\n\n")
                        }
                    } catch (e) {
                        log.error("Failed to parse events JSON: ${e.message}")
                    }
                }
                
                // Process Security Analysis
                def analysisJson = record.get('Observables Analysis')
                if (analysisJson && analysisJson != '[]') {
                    try {
                        def analyses = jsonSlurper.parseText(analysisJson)
                        analyses.each { analysis ->
                            def analysisUri = "<http://irkb.com/analysis/${incidentId}_${analysis['dataType']}>"
                            writer.write("    prov:wasGeneratedBy $analysisUri ;\n")
                            writer.write("$analysisUri a :SecurityAnalysis ;\n")
                            writer.write("    :hasSecurityAnalysisType \"${analysis['dataType']}\" ;\n")
                            writer.write("    :hasSecurityAnalysisDataValue \"${analysis['data']}\" ;\n")
                            writer.write("    :hasSecurityAnalysisDateTime \"${convertDate(analysis['createdAt'])}\"^^xsd:dateTime ;\n")
                            writer.write("    :hasSecurityAnalysisDetails \"${analysis['report'].replace('"', '\\"')}\" ;\n")
                            writer.write("    prov:wasPerformedBy [ a :Analyst ; :hasAnalystEmail \"${analysis['createdBy']}\" ] .\n\n")
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
