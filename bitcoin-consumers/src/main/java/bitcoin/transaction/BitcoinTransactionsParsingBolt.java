package bitcoin.transaction;

import bitcoin.common.Constants;
import bitcoin.common.ParsingBaseRichBolt;
import org.apache.storm.shade.org.json.simple.JSONObject;
import org.apache.storm.shade.org.json.simple.parser.JSONParser;
import org.apache.storm.shade.org.json.simple.parser.ParseException;
import org.apache.storm.task.OutputCollector;
import org.apache.storm.task.TopologyContext;
import org.apache.storm.topology.OutputFieldsDeclarer;
import org.apache.storm.tuple.Fields;
import org.apache.storm.tuple.Tuple;
import org.apache.storm.tuple.Values;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.text.SimpleDateFormat;
import java.util.Date;
import java.util.Map;

public class BitcoinTransactionsParsingBolt extends ParsingBaseRichBolt {

    private static final Logger LOG = LoggerFactory.getLogger(BitcoinTransactionsParsingBolt.class);

    private OutputCollector collector;
    private Map<String, String> parameters;
    private SimpleDateFormat sdf;
    private Double bpiEur = 0d;

    public BitcoinTransactionsParsingBolt(Map<String, String> parameters) {

        this.parameters = parameters;
        this.sdf = new SimpleDateFormat("yyyy-MM-dd'T'HH:mm:ss.SSS");
    }

    @Override
    public void prepare(Map map, TopologyContext topologyContext, OutputCollector collector) {

        LOG.debug("Preparing BitcoinTransactionsParsingBolt");
        this.collector = collector;
    }

    @Override
    public void execute(Tuple input) {

        LOG.debug("Executing BitcoinTransactionsParsingBolt tuple");
        try {
            this.process(input);
        } catch (Exception e) {

            LOG.error("BitcoinTransactionsParsingBolt Input parsing exception", e);
            this.collector.fail(input);
        }
    }

    @Override
    public void declareOutputFields(OutputFieldsDeclarer outputFieldsDeclarer) {

        LOG.debug("Declaring BitcoinTransactionsParsingBolt output fields");
        outputFieldsDeclarer.declare(new Fields(Constants.BTC_TX_FIELD_INDEX,
                Constants.BTC_TX_FIELD_TYPE,
                Constants.BTC_TX_FIELD_TX_ID,
                Constants.SOURCE));
    }

    @SuppressWarnings("unchecked")
    private void process(Tuple input) throws ParseException {

        LOG.debug("Processing BitcoinTransactionsParsingBolt input");
        LOG.debug("Source Component: " + input.getSourceComponent());

        if (Constants.BPI_SPOUT.equals(input.getSourceComponent())) {

            this.bpiEur = getBpiEurRate(input);
            LOG.debug("bpiEur: " + this.bpiEur);

        } else {

            JSONParser jsonParser = new JSONParser();
            JSONObject obj = (JSONObject) jsonParser.parse(input.getStringByField("value"));
            Date btcTimestamp = new Date((Long) obj.get("btc_timestamp") * 1000);
            LOG.debug("btcTimestamp: " + btcTimestamp);
            String txId = (String) obj.get("tx_id");
            LOG.debug("txId: " + txId);
            Double txBtcAmount = (Double) obj.get("tx_btc_amount");
            LOG.debug("txBtcAmount: " + txBtcAmount);
            Double txEurAmount = txBtcAmount * this.bpiEur;
            LOG.debug("txEurAmount: " + txEurAmount);

            JSONObject source = new JSONObject();
            source.put(Constants.BTC_TX_FIELD_DATE, sdf.format(btcTimestamp));
            source.put(Constants.BTC_TX_FIELD_TX_BTC_AMOUNT, txBtcAmount);
            if (this.bpiEur > 0)
                source.put(Constants.BTC_TX_FIELD_TX_EUR_AMOUNT, txEurAmount);

            LOG.debug("Emitting BitcoinTransactionsParsingBolt values");
            this.collector.emit(new Values(this.parameters.get(Constants.INDEX_NAME),
                    this.parameters.get(Constants.BTC_TX_TYPE),
                    txId,
                    source.toJSONString()));

            LOG.debug("Acknowledging BitcoinTransactionsParsingBolt input");
            this.collector.ack(input);
        }

    }
}
