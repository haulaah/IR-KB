import org.apache.commons.csv.*
import java.nio.charset.StandardCharsets
 
// Get the flow files
def flowFile = session.get()
if (!flowFile) return

try {
    // Process the CSV and generate RDF
    flowFile = session.write(flowFile, { inputStream, outputStream ->
        def reader = new InputStreamReader(inputStream, StandardCharsets.UTF_8)
        def writer = new OutputStreamWriter(outputStream, StandardCharsets.UTF_8)
        
        // Parse CSV with header
        def parser = CSVFormat.DEFAULT
            .withFirstRecordAsHeader()
            .withIgnoreEmptyLines()
            .withTrim()
            .parse(reader)
        
        // Write RDF header
        writer.write("""@prefix csiho: <http://gb.moreira.nom.br/csiho.owl#> .
@prefix xsd: <http://www.w3.org/2001/XMLSchema#> .
@prefix rdf: <http://www.w3.org/1999/02/22-rdf-syntax-ns#> .
@prefix rdfs: <http://www.w3.org/2000/01/rdf-schema#> .

""")

        // Process each record
        parser.records.each { record ->
            def id = record.get('id')
            def info = record.get('info')
            def date = record.get('date')
            def tags = record.get('tags')
            
            // Create URI
            def uri = "<http://example.org/security/event/${id}>"
            
            // Write RDF triples
            writer.write("${uri} a csiho:Security_Event, csiho:Incident")
            
            // Add specific event type
            if (info.contains("Brute Force")) writer.write(", csiho:Firewall_Event")
            else if (info.contains("Port Scan")) writer.write(", csiho:IPS_Event")
            else if (info.contains("Malware")) writer.write(", csiho:Antivirus_Event")
            else if (info.contains("Phishing")) writer.write(", csiho:Proxy_Event")
            
            writer.write(" ;\n")
            writer.write("    csiho:hasID \"${id}\" ;\n")
            writer.write("    rdfs:label \"${info.replace('"', '\\"')}\" ;\n")
            writer.write("    csiho:hasDate \"${date}\"^^xsd:date")
            
            // Add tags if present
            if (tags && !tags.trim().isEmpty()) {
                writer.write(" ;\n    csiho:hasTag \"${tags.replace('"', '\\"')}\"")
            }
            
            writer.write(" .\n\n")
        }
        
        writer.flush()
    } as StreamCallback)
    
    session.transfer(flowFile, REL_SUCCESS)
} catch (Exception e) {
    log.error("Processing failed: ${e.message}", e)
    session.transfer(flowFile, REL_FAILURE)
}
