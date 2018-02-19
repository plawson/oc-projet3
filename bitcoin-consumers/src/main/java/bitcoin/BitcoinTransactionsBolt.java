package bitcoin;

import org.apache.http.HttpHost;
import org.apache.storm.shade.org.json.simple.JSONObject;
import org.apache.storm.shade.org.json.simple.parser.JSONParser;
import org.apache.storm.task.OutputCollector;
import org.apache.storm.task.TopologyContext;
import org.apache.storm.topology.OutputFieldsDeclarer;
import org.apache.storm.topology.base.BaseRichBolt;
import org.apache.storm.tuple.Tuple;
import org.elasticsearch.action.index.IndexRequest;
import org.elasticsearch.action.index.IndexResponse;
import org.elasticsearch.client.RestClient;
import org.elasticsearch.client.RestHighLevelClient;
import org.elasticsearch.rest.RestStatus;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.Date;
import java.util.HashMap;
import java.util.Map;

public class BitcoinTransactionsBolt extends BaseRichBolt {

    private static final Logger LOGGER = LoggerFactory.getLogger(BitcoinTransactionsBolt.class);

    private OutputCollector collector;
    private RestHighLevelClient esClient;
    private String esHostname;
    private  int esPort;

    BitcoinTransactionsBolt(String esHostname, String esPort) {
        this.esHostname = esHostname;
        LOGGER.info("esHostname: " + this.esHostname);
        this.esPort = Integer.parseInt(esPort);
        LOGGER.info("esPort: " + this.esPort);
    }

    @Override
    public void prepare(Map map, TopologyContext topologyContext, OutputCollector collector) {
        this.collector = collector;
        LOGGER.info("Creating REST High Level Client...");
        RestClient restClient = RestClient.builder(new HttpHost(this.esHostname,
                this.esPort, "http"))
                .build();
        this.esClient = new RestHighLevelClient(restClient);
        LOGGER.info("REST High Level Client created.");
    }

    @Override
    public void execute(Tuple input) {

        LOGGER.info("Executing tuple");
        try {
            this.process(input);
            this.collector.ack(input);
        } catch (Exception e) {

            this.collector.fail(input);
        }
    }

    @Override
    public void declareOutputFields(OutputFieldsDeclarer outputFieldsDeclarer) {

    }

    private void process(Tuple input) throws Exception {
        JSONParser jsonParser = new JSONParser();
        JSONObject obj = (JSONObject)jsonParser.parse(input.getStringByField("value"));
        Date btcTimestamp = new Date((Long)obj.get("btc_timestamp"));
        LOGGER.info("btcTimestamp: " + btcTimestamp);
        String txId = (String)obj.get("tx_id");
        LOGGER.info("txId: " + txId);
        Double txBtcAmount = (Double)obj.get("tx_btc_amount");
        LOGGER.info("txBtcAmount: " + txBtcAmount);
        Double txEurAmount = (Double)obj.get("tx_eur_amount");
        LOGGER.info("txEurAmount: " + txEurAmount);

        Map<String, Object> jsonMap = new HashMap<>();
        jsonMap.put("btc_timestamp", btcTimestamp);
        jsonMap.put("tx_btc_amount", txBtcAmount);
        jsonMap.put("tx_eur_amount", txEurAmount);

        IndexRequest indexRequest = new IndexRequest("bitcoin_monitoring", "btc_tx", txId).source(jsonMap);
        IndexResponse indexResponse = this.esClient.index(indexRequest);
        if (!indexResponse.status().equals(RestStatus.ACCEPTED)) {
            throw new Exception("ES index failed: " + indexResponse.toString());
        }

    }
}
