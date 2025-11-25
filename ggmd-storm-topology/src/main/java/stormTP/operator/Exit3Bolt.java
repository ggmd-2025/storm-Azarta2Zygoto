// ...existing code...
package stormTP.operator;

import java.util.Map;
import java.util.logging.Logger;
import java.util.ArrayList;
import java.util.List;
import java.io.StringReader;

import javax.json.Json;
import javax.json.JsonArray;
import javax.json.JsonObject;
import javax.json.JsonReader;
import javax.json.JsonValue;
import javax.json.JsonNumber;
import javax.json.JsonString;
import javax.json.JsonObjectBuilder;
import javax.json.JsonArrayBuilder;

import org.apache.storm.task.OutputCollector;
import org.apache.storm.task.TopologyContext;
import org.apache.storm.topology.IRichBolt;
import org.apache.storm.topology.OutputFieldsDeclarer;
import org.apache.storm.tuple.Fields;
import org.apache.storm.tuple.Tuple;

import stormTP.stream.StreamEmiter;

public class Exit3Bolt implements IRichBolt {

    private static final long serialVersionUID = 4262369370788107342L;
    private OutputCollector collector;
    private final int port;
    private final StreamEmiter semit;

    public Exit3Bolt(int port) {
        this.port = port;
        this.semit = new StreamEmiter(this.port);
    }

    @Override
    @SuppressWarnings("rawtypes")
    public void prepare(Map stormConf, TopologyContext context, OutputCollector collector) {
        this.collector = collector;
    }

    @Override
    public void execute(Tuple t) {
        try {   
            // 1) Si le tuple contient un champ "json", parser et traiter les runners
            Object jsonField = null;
            try { jsonField = t.getValueByField("json"); } catch (Exception ignore) {}

            if (jsonField != null) {
                String payload = String.valueOf(jsonField);
                try (JsonReader jr = Json.createReader(new StringReader(payload))) {
                    JsonObject obj = jr.readObject();
                    JsonArray runners = (obj.containsKey("runners") && obj.get("runners").getValueType() == JsonValue.ValueType.ARRAY)
                                        ? obj.getJsonArray("runners")
                                        : null;

                    List<StringBuilder> list = new ArrayList<>();
                    if (runners != null) {
                        for (JsonValue rv : runners) {
                            if (rv.getValueType() != JsonValue.ValueType.OBJECT) continue;
                            JsonObject r = (JsonObject) rv;

                            Object idV = jsonValueToObject(r.get("id"));
                            Object topV = jsonValueToObject(r.get("top"));
                            Object rangV = jsonValueToObject(r.get("rang"));
                            Object totalV = jsonValueToObject(r.get("total"));
                            // GiveRankBolt utilise "maxcells" — adapter vers "maxcel" attendu ici
                            Object maxcelV = jsonValueToObject(r.get("maxcells"));

                            StringBuilder sb = new StringBuilder();
                            sb.append("{");
                            sb.append("\"id\":").append(renderValue(idV)).append(",");
                            sb.append("\"top\":").append(renderValue(topV)).append(",");
                            sb.append("\"rang\":").append(renderValue(rangV)).append(",");
                            sb.append("\"total\":").append(renderValue(totalV)).append(",");
                            sb.append("\"maxcel\":").append(renderValue(maxcelV));
                            sb.append("}");
                            list.add(sb);
                        }
                    }
                    JsonArrayBuilder outArrayBuilder = Json.createArrayBuilder();
                    for (StringBuilder sb : list) {
                        outArrayBuilder.add(sb.toString());
                    }

                    JsonObjectBuilder outBuilder = Json.createObjectBuilder();
                    outBuilder.add("runners", outArrayBuilder);
                    JsonObject outObj = outBuilder.build();
                    String outJson = outObj.toString();
                    this.semit.send(outJson);
                    collector.ack(t);
                } catch (Exception ex) {
                    // si parsing échoue, on retombe sur le comportement legacy ci‑dessous
                }
            }
        } catch (Exception e) {
            if (collector != null) collector.fail(t);
        }
    }

    private static Object safeGet(Tuple t, String field) {
        try {
            return t.getValueByField(field);
        } catch (Exception e) {
            return null;
        }
    }

    private static String renderValue(Object v) {
        if (v == null) return "null";
        if (v instanceof Number || v instanceof Boolean) return v.toString();
        // sinon traiter comme chaîne
        return "\"" + jsonEscape(String.valueOf(v)) + "\"";
    }

    private static String jsonEscape(String s) {
        if (s == null) return "";
        StringBuilder out = new StringBuilder();
        for (int i = 0; i < s.length(); i++) {
            char c = s.charAt(i);
            switch (c) {
                case '"': out.append("\\\""); break;
                case '\\': out.append("\\\\"); break;
                case '\b': out.append("\\b"); break;
                case '\f': out.append("\\f"); break;
                case '\n': out.append("\\n"); break;
                case '\r': out.append("\\r"); break;
                case '\t': out.append("\\t"); break;
                default:
                    if (c < 0x20 || c > 0x7E) {
                        out.append(String.format("\\u%04x", (int)c));
                    } else {
                        out.append(c);
                    }
            }
        }
        return out.toString();
    }
    private static Object jsonValueToObject(JsonValue v) {
        if (v == null || v.getValueType() == JsonValue.ValueType.NULL) return null;
        switch (v.getValueType()) {
            case STRING:
                return ((JsonString) v).getString();
            case NUMBER:
                JsonNumber jn = (JsonNumber) v;
                // retourner un Integer si possible, sinon Long/Double via numberValue()
                Number n = jn.isIntegral() ? jn.longValue() : jn.doubleValue();
                return n;
            case TRUE:
                return Boolean.TRUE;
            case FALSE:
                return Boolean.FALSE;
            case OBJECT:
            case ARRAY:
            default:
                return v.toString();
        }
    }

    @Override
    public void declareOutputFields(OutputFieldsDeclarer declarer) {
        declarer.declare(new Fields("json"));
    }

    @Override
    public Map<String, Object> getComponentConfiguration() {
        return null;
    }

    @Override
    public void cleanup() {
        // fermer semit si nécessaire
    }
}
// ...existing code...