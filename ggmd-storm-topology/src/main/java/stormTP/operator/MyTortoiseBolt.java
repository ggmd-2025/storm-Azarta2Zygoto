package stormTP.operator;


import java.util.Map;
import java.util.Set;
import java.util.HashSet;
import java.util.logging.Logger;
//import java.util.logging.Logger;

import org.apache.storm.task.OutputCollector;
import org.apache.storm.task.TopologyContext;
import org.apache.storm.topology.IRichBolt;
import org.apache.storm.topology.OutputFieldsDeclarer;
import org.apache.storm.tuple.Fields;
import org.apache.storm.tuple.Tuple;

import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.fasterxml.jackson.databind.node.ArrayNode;
import com.fasterxml.jackson.databind.node.ObjectNode;

import stormTP.stream.StreamEmiter;


public class MyTortoiseBolt implements IRichBolt {

	private static final long serialVersionUID = 4262369370788107342L;
	private static Logger logger = Logger.getLogger("ExitBolt");
	private OutputCollector collector;
	int port = -1;
	StreamEmiter semit = null;
    private final ObjectMapper mapper = new ObjectMapper();
	
	public MyTortoiseBolt (int port) {
		this.port = port;
		this.semit = new StreamEmiter(this.port);
		
	}
	
	/* (non-Javadoc)
	 * @see backtype.storm.topology.IRichBolt#execute(backtype.storm.tuple.Tuple)
	 */
	public void execute(Tuple t) {
	
        String n = (t.getValueByField("json") != null) ? t.getValueByField("json").toString() : null;
        logger.info("[MyTortoiseBolt] EXEC : " + n);
		if (n == null) {
            collector.ack(t);
            return;
        }
		
		try {
            JsonNode root = mapper.readTree(n);
            JsonNode runnersNode = root.get("runners");

            ArrayNode filtered = mapper.createArrayNode();

            // valeur à filtrer
            String runnerValue = "3";
            Set<String> allowedIds = new HashSet<>();
            allowedIds.add(runnerValue);

        	if (runnersNode != null && runnersNode.isArray()) {
                for (JsonNode r : runnersNode) {
                    JsonNode idNode = r.get("id");
                    String idStr = (idNode != null) ? idNode.asText() : null;
                    // si allowedIds est vide ou idStr est null ou id est autorisé -> garder
                    if (allowedIds == null || idStr == null || allowedIds.contains(idStr)) {
                        ObjectNode exit = mapper.createObjectNode();
                        exit.put("id", idStr);
                        if (r.has("top"))
                            exit.set("top", r.get("top"));
                        exit.put("nom", "Caroline");

                        int tour = r.has("tour") && r.get("tour").canConvertToInt() ? r.get("tour").asInt() : 0;
                        int maxCells = r.has("maxcel") && r.get("maxcel").canConvertToInt() ? r.get("maxcel").asInt() : 0;
                        int currentCells = r.has("cellule") && r.get("cellule").canConvertToInt() ? r.get("cellule").asInt() : 0;

                        int nbCellsParcourus = currentCells + (tour * maxCells);
                        exit.put("nbCellsParcourus", nbCellsParcourus);

                        if (r.has("total"))
                            exit.set("total", r.get("total"));
                        exit.put("maxcells", maxCells);

                        filtered.add(exit);
                    }
                }
            }

            // construire sortie: copier l'objet racine et remplacer runners
            ObjectNode outNode = (root.isObject()) ? ((ObjectNode) root).deepCopy() : mapper.createObjectNode();
            outNode.set("runners", filtered);
            String outStr = mapper.writeValueAsString(outNode);

			this.semit.send(outStr);
			collector.ack(t);
        } catch (Exception e) {
            logger.severe("[Exit2Bolt] Error: " + e.getMessage());
            collector.fail(t);
        }
		
	}
	

	
	/* (non-Javadoc)
	 * @see backtype.storm.topology.IComponent#declareOutputFields(backtype.storm.topology.OutputFieldsDeclarer)
	 */
	public void declareOutputFields(OutputFieldsDeclarer arg0) {
		arg0.declare(new Fields("json"));
	}
		

	/* (non-Javadoc)
	 * @see backtype.storm.topology.IComponent#getComponentConfiguration()
	 */
	public Map<String, Object> getComponentConfiguration() {
		return null;
	}

	/* (non-Javadoc)
	 * @see backtype.storm.topology.IBasicBolt#cleanup()
	 */
	public void cleanup() {
		
	}
	
	/* (non-Javadoc)
	 * @see backtype.storm.topology.IRichBolt#prepare(java.util.Map, backtype.storm.task.TopologyContext, backtype.storm.task.OutputCollector)
	 */
	@SuppressWarnings("rawtypes")
	public void prepare(Map arg0, TopologyContext context, OutputCollector collector) {
		this.collector = collector;
	}
}