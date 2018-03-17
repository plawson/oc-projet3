package bitcoin.block;

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

public class BitcoinBlockParsingBolt extends ParsingBaseRichBolt {

    private static final Logger LOG = LoggerFactory.getLogger(BitcoinBlockParsingBolt.class);

    private OutputCollector collector;
    private Map<String, String> parameters;
    private SimpleDateFormat sdf;
    private Double bpiEur = 0d;

    public BitcoinBlockParsingBolt(Map<String, String> parameters) {
        this.parameters = parameters;
        this.sdf = new SimpleDateFormat("yyyy-MM-dd'T'HH:mm:ss.SSS");
    }

    @Override
    public void prepare(Map map, TopologyContext topologyContext, OutputCollector collector) {
        LOG.debug("Preparing BitcoinBlockParsingBolt");
        this.collector = collector;
    }

    @Override
    public void execute(Tuple input) {
        LOG.debug("Executing BitcoinBlockParsingBolt tuple");

        try {
            this.process(input);
        } catch (Exception e) {
            LOG.error("BitcoinBlockParsingBolt Input parsing exception", e);
            this.collector.fail(input);
        }
    }

    @Override
    public void declareOutputFields(OutputFieldsDeclarer outputFieldsDeclarer) {
        LOG.debug("Declaring BitcoinBlockParsingBolt output fields");
        outputFieldsDeclarer.declare(new Fields(Constants.BTC_BLK_FIELD_INDEX,
                Constants.BTC_BLK_FIELD_TYPE,
                Constants.BTC_BLK_FIELD_TX_ID,
                Constants.SOURCE));
    }

    @SuppressWarnings("unchecked")
    private void process(Tuple input) throws ParseException {
        LOG.debug("Processing BitcoinBlockParsingBolt input");
        LOG.debug("Source Component: " + input.getSourceComponent());

        if (Constants.BPI_SPOUT.equals(input.getSourceComponent())) {

            this.bpiEur = getBpiEurRate(input);
            LOG.debug("bpiEur: " + this.bpiEur);

        } else {

            JSONParser jsonParser = new JSONParser();
            JSONObject obj = (JSONObject) jsonParser.parse(input.getStringByField("value"));
            Date btcTimestamp = new Date((Long) obj.get("btc_timestamp") * 1000);
            LOG.debug("btcTimestamp: " + btcTimestamp);
            String blkId = (String) obj.get("blk_id");
            LOG.debug("txId: " + blkId);
            Double blkBtcReward = (Double) obj.get("blk_btc_reward");
            LOG.debug("blkBtcReward: " + blkBtcReward);
            Double blkEurReward = blkBtcReward * this.bpiEur;
            LOG.debug("blkEurReward: " + blkEurReward);
            String blkOwner = (String)obj.get("blk_owner");
            LOG.debug("blkOwner: " + blkOwner);

            JSONObject source = new JSONObject();
            source.put(Constants.BTC_BLK_FIELD_DATE, sdf.format(btcTimestamp));
            source.put(Constants.BTC_BLK_FIELD_BTC_REWARD, blkBtcReward);
            if (this.bpiEur > 0)
                source.put(Constants.BTC_BLK_FIELD_EUR_REWARD, blkEurReward);
            source.put(Constants.BTC_BLK_FIELD_OWNER, blkOwner);

            LOG.debug("Emitting BitcoinBlockParsingBolt values");
            this.collector.emit(new Values(this.parameters.get(Constants.INDEX_NAME),
                    this.parameters.get(Constants.BTC_BLK_TYPE),
                    blkId,
                    source.toJSONString()));
        }

        LOG.debug("Acknowledging BitcoinBlockParsingBolt input");
        this.collector.ack(input);
    }
}
