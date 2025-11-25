// ...existing code...
package stormTP.operator;

import java.util.Map;

import org.apache.storm.task.OutputCollector;
import org.apache.storm.task.TopologyContext;
import org.apache.storm.topology.IRichBolt;
import org.apache.storm.topology.OutputFieldsDeclarer;
import org.apache.storm.tuple.Fields;
import org.apache.storm.tuple.Tuple;

import stormTP.stream.StreamEmiter;

public class Exit2Bolt implements IRichBolt {

    private static final long serialVersionUID = 4262369370788107342L;
    private OutputCollector collector;
    private final int port;
    private final StreamEmiter semit;

    public Exit2Bolt(int port) {
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
            Object idV = safeGet(t, "id");
            Object topV = safeGet(t, "top");
            Object nomV = safeGet(t, "nom");
            Object nbCellsV = safeGet(t, "nbCellsParcourus");
            Object totalV = safeGet(t, "total");
            Object maxcelV = safeGet(t, "maxcel");

            StringBuilder sb = new StringBuilder();
            sb.append("{");

            sb.append("\"id\":").append(renderValue(idV)).append(",");
            sb.append("\"top\":").append(renderValue(topV)).append(",");
            sb.append("\"nom\":").append(renderValue(nomV)).append(",");
            sb.append("\"nbCellsParcourus\":").append(renderValue(nbCellsV)).append(",");
            sb.append("\"total\":").append(renderValue(totalV)).append(",");
            sb.append("\"maxcel\":").append(renderValue(maxcelV));

            sb.append("}");

            String outJson = sb.toString();
            this.semit.send(outJson);
            collector.ack(t);
        } catch (Exception e) {
            // ne pas laisser une exception non catchée empêcher le worker
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