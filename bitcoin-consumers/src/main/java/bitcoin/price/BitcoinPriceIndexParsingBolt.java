package bitcoin.price;

import bitcoin.common.Constants;
import org.apache.storm.shade.org.json.simple.JSONObject;
import org.apache.storm.shade.org.json.simple.parser.JSONParser;
import org.apache.storm.shade.org.json.simple.parser.ParseException;
import org.apache.storm.task.OutputCollector;
import org.apache.storm.task.TopologyContext;
import org.apache.storm.topology.OutputFieldsDeclarer;
import org.apache.storm.topology.base.BaseRichBolt;
import org.apache.storm.tuple.Fields;
import org.apache.storm.tuple.Tuple;
import org.apache.storm.tuple.Values;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.text.SimpleDateFormat;
import java.util.Date;
import java.util.Map;
import java.util.UUID;

public class BitcoinPriceIndexParsingBolt extends BaseRichBolt {

    private static final Logger LOG = LoggerFactory.getLogger(BitcoinPriceIndexParsingBolt.class);

    private OutputCollector collector;
    private Map<String, String> parameters;
    private SimpleDateFormat sdf;

    public BitcoinPriceIndexParsingBolt(Map<String, String> parameters) {
        this.parameters = parameters;
        this.sdf = new SimpleDateFormat("yyyy-MM-dd'T'HH:mm:ss.SSS");
    }

    @Override
    public void prepare(Map map, TopologyContext topologyContext, OutputCollector collector) {

        LOG.debug("Preparing BitcoinPriceIndexParsingBolt");
        this.collector = collector;
    }

    @Override
    public void execute(Tuple input) {

        LOG.debug("Executing BitcoinPriceIndexParsingBolt tuple");

        try {
            this.process(input);

        } catch (Exception e) {

            LOG.error("BitcoinPriceIndexParsingBolt Input parsing exception", e);
            this.collector.fail(input);
        }
    }

    @Override
    public void declareOutputFields(OutputFieldsDeclarer outputFieldsDeclarer) {

        LOG.debug("Declaring BitcoinPriceIndexParsingBolt output fields");
        outputFieldsDeclarer.declare(new Fields(Constants.BPI_FIELD_INDEX,
                Constants.BPI_FIELD_TYPE,
                Constants.BPI_FIELD_ID,
                Constants.SOURCE));
    }

    @SuppressWarnings("unchecked")
    private void process(Tuple input) throws ParseException {

        LOG.debug("Processing BitcoinPriceIndexParsingBolt input");
        JSONParser jsonParser = new JSONParser();
        JSONObject obj = (JSONObject)jsonParser.parse(input.getStringByField("value"));
        Date btcTimestamp = new Date((Long) obj.get("btc_timestamp") * 1000);
        LOG.debug("btcTimestamp: " + btcTimestamp);
        String bpiId = UUID.randomUUID().toString();
        LOG.debug("bpiID: " + bpiId);
        Double bpiRate = (Double)obj.get("rate_float");
        LOG.debug("bpiRate: " + bpiRate);
        String bpiCurrency = (String)obj.get("currency");
        LOG.debug("bpiCurrenct: " + bpiCurrency);

        JSONObject source = new JSONObject();
        source.put(Constants.BPI_FIELD_DATE, sdf.format(btcTimestamp));
        source.put(Constants.BPI_FIELD_RATE, bpiRate);
        source.put(Constants.BPI_FIELD_CURRENCY, bpiCurrency);

        LOG.debug("Emitting BitcoinPriceIndexParsingBolt values");
        this.collector.emit(new Values(this.parameters.get(Constants.INDEX_NAME),
                this.parameters.get(Constants.BPI_TYPE),
                bpiId,
                source.toJSONString()));

        LOG.debug("Acknowledging BitcoinPriceIndexParsingBolt input");
        this.collector.ack(input);
    }
}
