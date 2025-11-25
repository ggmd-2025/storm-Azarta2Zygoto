package stormTP.operator;

import java.util.Map;
import java.util.logging.Logger;
import java.util.ArrayList;
import java.util.List;
import java.util.Comparator;
import java.io.StringReader;

import javax.json.Json;
import javax.json.JsonArray;
import javax.json.JsonArrayBuilder;
import javax.json.JsonObject;
import javax.json.JsonObjectBuilder;
import javax.json.JsonReader;
import javax.json.JsonValue;
import javax.json.JsonNumber;

import org.apache.storm.task.OutputCollector;
import org.apache.storm.task.TopologyContext;
import org.apache.storm.topology.IRichBolt;
import org.apache.storm.topology.OutputFieldsDeclarer;
import org.apache.storm.tuple.Fields;
import org.apache.storm.tuple.Tuple;

import stormTP.stream.StreamEmiter;


public class GiveRankBolt implements IRichBolt {

	private static final long serialVersionUID = 4262369370788107342L;
	private static Logger logger = Logger.getLogger("ExitBolt");
	private OutputCollector collector;
	int port = -1;
    private static final int ALLOW_ID = 3;
	StreamEmiter semit = null;
	
	public GiveRankBolt (int port) {
		this.port = port;
		this.semit = new StreamEmiter(this.port);
	}
	
	/* (non-Javadoc)
	 * @see backtype.storm.topology.IRichBolt#execute(backtype.storm.tuple.Tuple)
	 */
	public void execute(Tuple t) {
        Object vf = null;
		logger.info("[GiveRankBolt] EXEC");
        try { vf = t.getValueByField("json"); } catch (Exception e) { collector.ack(t); return; }
        if (vf == null) { collector.ack(t); return; }
        String n = vf.toString();

        try (JsonReader jr = Json.createReader( new StringReader(n) )) {
            JsonObject obj = jr.readObject();

            JsonArray runners = (obj.containsKey("runners") && obj.get("runners").getValueType() == JsonValue.ValueType.ARRAY)
                                ? obj.getJsonArray("runners")
                                : null;
            JsonArrayBuilder filteredBuilder = Json.createArrayBuilder();

			if (runners != null) {
                for (JsonValue runnerVal : runners) {
                    if (runnerVal.getValueType() != JsonValue.ValueType.OBJECT) continue;
                    JsonObject r = (JsonObject) runnerVal; // cast correct pour javax.json

                    int id = getIntSafely(r, "id", Integer.MIN_VALUE);
                    String idStr = getStringSafely(r, "id");

                    JsonObjectBuilder exit = Json.createObjectBuilder();

                    if (id != Integer.MIN_VALUE) exit.add("id", id);
                    else if (idStr != null) exit.add("id", idStr);
                    else exit.addNull("id");

                    if (r.containsKey("top")) exit.add("top", r.get("top"));

                    exit.add("nom", "Caroline");

                    int tour = getIntSafely(r, "tour", 0);
                    int maxCells = getIntSafely(r, "maxcel", 0);
                    int currentCells = getIntSafely(r, "cellule", 0);
					logger.info("[MyTortoiseBolt] tour: " + tour + " maxCells: " + maxCells + " currentCells: " + currentCells);

                    int nbCellsParcourus = currentCells + (tour * maxCells);
                    exit.add("nbCellsParcourus", nbCellsParcourus);

                    if (r.containsKey("total")) exit.add("total", r.get("total"));

                    exit.add("maxcells", maxCells);

                    filteredBuilder.add(exit);
                }
            }
            JsonArray filteredArray = filteredBuilder.build();

            // 1) Convert JsonArray to List<JsonObject>
            List<JsonObject> list = new ArrayList<>();
            for (JsonValue v : filteredArray) {
                if (v.getValueType() == JsonValue.ValueType.OBJECT) {
                    list.add((JsonObject) v);
                }
            }

            // 2) Sort by "nbCellsParcourus"
            list.sort(Comparator.comparingInt(o -> o.getInt("nbCellsParcourus")));

            // 3) Create a new sorted builder
            JsonArrayBuilder sortedBuilder = Json.createArrayBuilder();
            list.forEach(sortedBuilder::add);

            // 4) Use the sorted builder
            JsonArray sortedArray = sortedBuilder.build();

            // 5) Create corrected/ranked array
            JsonArrayBuilder correctedBuilder = Json.createArrayBuilder();
            int rank = 0;
            for (JsonValue jv : sortedArray) {
                JsonObject runnerObj = (JsonObject) jv;
                JsonObjectBuilder newObj = Json.createObjectBuilder();
                newObj.add("id", runnerObj.get("id"));
                if (runnerObj.containsKey("top")) newObj.add("top", runnerObj.get("top"));
                newObj.add("rang", ++rank);
                if (runnerObj.containsKey("total")) newObj.add("total", runnerObj.get("total"));
                newObj.add("maxcells", runnerObj.getInt("maxcells", 0));
                correctedBuilder.add(newObj);
            }

            JsonObjectBuilder outBuilder = Json.createObjectBuilder();
            for (String key : obj.keySet()) {
                if ("runners".equals(key)) continue;
                outBuilder.add(key, obj.get(key));
            }
            outBuilder.add("runners", correctedBuilder);

            JsonObject outObj = outBuilder.build();
            String outStr = outObj.toString();

            logger.info("[GiveRankBolt] Output JSON: " + outStr);
			collector.emit(t, new org.apache.storm.tuple.Values(outStr));
            collector.ack(t);
        } catch (Exception e) {
            logger.severe("[GiveRankBolt] Error: " + e.getMessage());
            collector.fail(t);
        }
    }
	


	private static int getIntSafely(JsonObject obj, String key, int def) {
        if (obj == null || key == null) return def;
        try {
            if (!obj.containsKey(key)) return def;
            JsonValue v = obj.get(key);
            if (v == null) return def;
            if (v.getValueType() == JsonValue.ValueType.NUMBER) {
                JsonNumber num = obj.getJsonNumber(key);
                return num.intValue();
            }
            if (v.getValueType() == JsonValue.ValueType.STRING) {
                String s = obj.getString(key, null);
                if (s == null) return def;
                return Integer.parseInt(s);
            }
        } catch (Exception ignored) {}
        return def;
    }

    private static String getStringSafely(JsonObject obj, String key) {
        if (obj == null || key == null) return null;
        try {
            if (!obj.containsKey(key)) return null;
            JsonValue v = obj.get(key);
            if (v == null) return null;
            if (v.getValueType() == JsonValue.ValueType.STRING) return obj.getString(key, null);
            // pour nombre ou littéral, renvoyer la représentation textuelle sans guillemets
            return obj.get(key).toString();
        } catch (Exception ignored) {}
        return null;
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