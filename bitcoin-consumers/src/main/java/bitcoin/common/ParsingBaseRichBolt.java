package bitcoin.common;

import org.apache.storm.shade.org.json.simple.JSONObject;
import org.apache.storm.shade.org.json.simple.parser.JSONParser;
import org.apache.storm.shade.org.json.simple.parser.ParseException;
import org.apache.storm.topology.base.BaseRichBolt;
import org.apache.storm.tuple.Tuple;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public abstract class ParsingBaseRichBolt extends BaseRichBolt {

    private static final Logger LOG = LoggerFactory.getLogger(ParsingBaseRichBolt.class);

    protected Double getBpiEurRate(Tuple input) throws ParseException {
        LOG.debug("getBpiEurRate");
        JSONParser jsonParser = new JSONParser();
        JSONObject obj = (JSONObject) jsonParser.parse(input.getStringByField("value"));
        return (Double) obj.get("rate_float");
    }
}
