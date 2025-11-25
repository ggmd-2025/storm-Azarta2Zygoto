package stormTP.operator;

import java.util.Map;
import java.util.HashMap;
import java.io.StringReader;

import javax.json.Json;
import javax.json.JsonArray;
import javax.json.JsonObject;
import javax.json.JsonReader;
import javax.json.JsonValue;
import javax.json.JsonNumber;
import javax.json.JsonString;

import org.apache.storm.task.OutputCollector;
import org.apache.storm.task.TopologyContext;
import org.apache.storm.topology.IRichBolt;
import org.apache.storm.topology.OutputFieldsDeclarer;
import org.apache.storm.tuple.Fields;
import org.apache.storm.tuple.Tuple;
import org.apache.storm.tuple.Values;

import stormTP.stream.StreamEmiter;

public class ComputeBonusBolt implements IRichBolt {

    private static final long serialVersionUID = 1L;
    // fenêtre en nombre de tops après laquelle on calcule le bonus
    private static final int WINDOW_SIZE = 15;

    private OutputCollector collector;
    private final int port;
    private final StreamEmiter semit;

    // état par tortue (clé: id string)
    private final Map<String, Integer> topsCount = new HashMap<>();
    private final Map<String, Integer> firstTopInWindow = new HashMap<>();
    private final Map<String, Integer> cumulativeScore = new HashMap<>();

    public ComputeBonusBolt() { this(-1); }

    public ComputeBonusBolt(int port) {
        this.port = port;
        this.semit = (port > 0) ? new StreamEmiter(port) : null;
    }

    @SuppressWarnings("rawtypes")
    @Override
    public void prepare(Map stormConf, TopologyContext context, OutputCollector collector) {
        this.collector = collector;
    }

    @Override
    public void execute(Tuple t) {
        try {
            Object jsonField = null;
            try { jsonField = t.getValueByField("json"); } catch (Exception ignored) {}

            if (jsonField != null) {
                String payload = String.valueOf(jsonField);
                try (JsonReader jr = Json.createReader(new StringReader(payload))) {
                    JsonObject obj = jr.readObject();
                    JsonArray runners = (obj.containsKey("runners") && obj.get("runners").getValueType() == JsonValue.ValueType.ARRAY)
                                        ? obj.getJsonArray("runners")
                                        : null;
                    if (runners != null) {
                        for (JsonValue rv : runners) {
                            JsonObject rObj = null;
                            if (rv.getValueType() == JsonValue.ValueType.OBJECT) {
                                rObj = (JsonObject) rv;
                            } else if (rv.getValueType() == JsonValue.ValueType.STRING) {
                                String s = ((JsonString) rv).getString();
                                try (JsonReader jr2 = Json.createReader(new StringReader(s))) {
                                    rObj = jr2.readObject();
                                } catch (Exception ex) {
                                    continue;
                                }
                            } else {
                                continue;
                            }
                            handleRunner(rObj);
                        }
                        collector.ack(t);
                        return;
                    }
                } catch (Exception ex) {
                    // parsing failed -> fallthrough to legacy fields handling
                }
            }

            // legacy: accept tuples with fields id, top, rang, total
            Object idV = safeGet(t, "id");
            Object topV = safeGet(t, "top");
            Object rangV = safeGet(t, "rang");
            Object totalV = safeGet(t, "total");
            if (idV != null && topV != null) {
                JsonObject.Builder tmp = Json.createObjectBuilder();
                // build minimal JsonObject for reuse of handler
                tmp.add("id", idV.toString());
                tmp.add("top", topV.toString());
                if (rangV != null) tmp.add("rang", rangV.toString());
                if (totalV != null) tmp.add("total", totalV.toString());
                JsonObject rObj = tmp.build();
                handleRunner(rObj);
            }
            collector.ack(t);
        } catch (Exception e) {
            if (collector != null) collector.fail(t);
        }
    }

    private void handleRunner(JsonObject rObj) {
        String id = jsonValueToString(rObj.get("id"));
        if (id == null) return;
        Integer top = jsonValueToInt(rObj.get("top"), null);
        Integer rang = jsonValueToInt(rObj.get("rang"), null);
        Integer total = jsonValueToInt(rObj.get("total"), null);

        if (top == null) return;

        int count = topsCount.getOrDefault(id, 0) + 1;
        topsCount.put(id, count);

        if (!firstTopInWindow.containsKey(id)) {
            firstTopInWindow.put(id, top);
        }

        // lorsque la fenêtre est atteinte, calculer le bonus et émettre
        if (count % WINDOW_SIZE == 0) {
            int firstTop = firstTopInWindow.getOrDefault(id, top);
            int lastTop = firstTop + (WINDOW_SIZE - 1);
            int bonus = 0;
            if (total != null && rang != null) {
                bonus = Math.max(0, total - rang);
            }
            int newScore = cumulativeScore.getOrDefault(id, 0) + bonus;
            cumulativeScore.put(id, newScore);

            String topsLabel = firstTop + "-" + lastTop;

            // émettre tuple (id, tops, score)
            collector.emit(new Values(id, topsLabel, newScore));

            // envoyer également en sortie via StreamEmiter si configuré (JSON minimal)
            if (semit != null) {
                String out = String.format("{\"id\":%s,\"tops\":\"%s\",\"score\":%d}", jsonEscapeForEmit(id), topsLabel, newScore);
                semit.send(out);
            }

            // reset start of next window
            firstTopInWindow.remove(id);
        }
    }

    private static Object safeGet(Tuple t, String field) {
        try { return t.getValueByField(field); } catch (Exception e) { return null; }
    }

    private static String jsonValueToString(JsonValue v) {
        if (v == null || v.getValueType() == JsonValue.ValueType.NULL) return null;
        if (v.getValueType() == JsonValue.ValueType.STRING) return ((JsonString) v).getString();
        if (v.getValueType() == JsonValue.ValueType.NUMBER) {
            JsonNumber jn = (JsonNumber) v;
            // return integral without .0 when possible
            if (jn.isIntegral()) return Long.toString(jn.longValue());
            return jn.toString();
        }
        return v.toString();
    }

    private static Integer jsonValueToInt(JsonValue v, Integer def) {
        if (v == null || v.getValueType() == JsonValue.ValueType.NULL) return def;
        try {
            if (v.getValueType() == JsonValue.ValueType.NUMBER) {
                return ((JsonNumber) v).intValue();
            } else if (v.getValueType() == JsonValue.ValueType.STRING) {
                String s = ((JsonString) v).getString();
                return Integer.parseInt(s);
            } else {
                return def;
            }
        } catch (Exception e) { return def; }
    }

    private static String jsonEscapeForEmit(String s) {
        if (s == null) return "null";
        // if id is numeric string, keep as is, else quote
        try { Long.parseLong(s); return s; } catch (Exception ex) {}
        return "\"" + s.replace("\"", "\\\"") + "\"";
    }

    @Override
    public void declareOutputFields(OutputFieldsDeclarer declarer) {
        declarer.declare(new Fields("id", "tops", "score"));
    }

    @Override
    public Map<String, Object> getComponentConfiguration() { return null; }

    @Override
    public void cleanup() {
        // persist state if nécessaire
    }
}