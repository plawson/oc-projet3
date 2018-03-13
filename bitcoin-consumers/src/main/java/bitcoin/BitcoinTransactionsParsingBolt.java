package bitcoin;

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

public class BitcoinTransactionsParsingBolt extends BaseRichBolt {

    private static final Logger LOGGER = LoggerFactory.getLogger(BitcoinTransactionsParsingBolt.class);

    private OutputCollector collector;
    private Map<String, String> peremeters;
    private SimpleDateFormat sdf;

    public BitcoinTransactionsParsingBolt(Map<String, String> parameters) {

        this.peremeters = parameters;
        this.sdf = new SimpleDateFormat("yyyy-MM-dd'T'HH:mm:ss.SSS");
    }

    @Override
    public void prepare(Map map, TopologyContext topologyContext, OutputCollector collector) {

        LOGGER.debug("Preparing bolt");
        this.collector = collector;
    }

    @Override
    public void execute(Tuple input) {

        LOGGER.debug("Executing tuple");
        try {
            this.process(input);
        } catch (ParseException pe) {

            LOGGER.error("Input parsing exception", pe);
            this.collector.fail(input);
        }
    }

    @Override
    public void declareOutputFields(OutputFieldsDeclarer outputFieldsDeclarer) {

        LOGGER.debug("Declaring output fields");
        outputFieldsDeclarer.declare(new Fields(Constants.BTC_TX_FIELD_INDEX,
                Constants.BTC_TX_FIELD_TYPE,
                Constants.BTC_TX_FIELD_TX_ID,
                Constants.SOURCE));
    }

    private void process(Tuple input) throws ParseException {

        LOGGER.debug("Processing input");
        JSONParser jsonParser = new JSONParser();
        JSONObject obj = (JSONObject)jsonParser.parse(input.getStringByField("value"));
        Date btcTimestamp = new Date((Long)obj.get("btc_timestamp") * 1000);
        LOGGER.debug("btcTimestamp: " + btcTimestamp);
        String txId = (String)obj.get("tx_id");
        LOGGER.debug("txId: " + txId);
        Double txBtcAmount = (Double)obj.get("tx_btc_amount");
        LOGGER.debug("txBtcAmount: " + txBtcAmount);
        Double txEurAmount = (Double)obj.get("tx_eur_amount");
        LOGGER.debug("txEurAmount: " + txEurAmount);

        JSONObject source = new JSONObject();
        source.put(Constants.BTC_TX_FIELD_DATE, sdf.format(btcTimestamp));
        source.put(Constants.BTC_TX_FIELD_TX_BTC_AMOUNT, txBtcAmount);
        source.put(Constants.BTC_TX_FIELD_TX_EUR_AMOUNT, txEurAmount);

        LOGGER.debug("Emitting values");
        this.collector.emit(new Values(this.peremeters.get(Constants.INDEX_NAME),
                this.peremeters.get(Constants.BTC_TX_TYPE),
                txId,
                source.toJSONString()));

        LOGGER.debug("Acknowledging input");
        this.collector.ack(input);

    }
}
