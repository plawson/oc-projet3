package bitcoin;

import bitcoin.block.BitcoinBlockEsTupleMapper;
import bitcoin.block.BitcoinBlockParsingBolt;
import bitcoin.common.Constants;
import bitcoin.price.BitcoinPriceIndexEsTupleMapper;
import bitcoin.price.BitcoinPriceIndexParsingBolt;
import bitcoin.transaction.BitcoinTransactionEsTupleMapper;
import bitcoin.transaction.BitcoinTransactionsParsingBolt;
import org.apache.http.Header;
import org.apache.http.entity.ContentType;
import org.apache.http.message.BasicHeader;
import org.apache.http.protocol.HTTP;
import org.apache.storm.Config;
import org.apache.storm.LocalCluster;
import org.apache.storm.StormSubmitter;
import org.apache.storm.elasticsearch.bolt.EsIndexBolt;
import org.apache.storm.elasticsearch.common.EsConfig;
import org.apache.storm.elasticsearch.common.EsTupleMapper;
import org.apache.storm.generated.AlreadyAliveException;
import org.apache.storm.generated.AuthorizationException;
import org.apache.storm.generated.InvalidTopologyException;
import org.apache.storm.generated.StormTopology;
import org.apache.storm.kafka.spout.KafkaSpout;
import org.apache.storm.kafka.spout.KafkaSpoutConfig;
import org.apache.storm.topology.TopologyBuilder;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.net.InetAddress;
import java.net.UnknownHostException;
import java.util.HashMap;
import java.util.Map;

/**
 * Bitcoin Monitoring
 *
 */
public class BitcoinMonitoring {

    private static final Logger LOG = LoggerFactory.getLogger(BitcoinMonitoring.class);

    private Map<String, String> parameters;

    private BitcoinMonitoring() throws UnknownHostException {
        // Parameters Map
        this.parameters = new HashMap<>();
        // Get topics information
        LOG.info("Getting Topics information...");
        this.parameters.put(Constants.BTC_TX_TOPIC, System.getenv("BTC_TX_TOPIC_NAME"));
        LOG.info(Constants.BTC_TX_TOPIC + ": " + this.parameters.get(Constants.BTC_TX_TOPIC));
        this.parameters.put(Constants.BTC_BLK_TOPIC, System.getenv("BTC_BLK_TOPIC_NAME"));
        LOG.info(Constants.BTC_BLK_TOPIC + ": " + this.parameters.get(Constants.BTC_BLK_TOPIC));
        this.parameters.put(Constants.BPI_TOPIC, System.getenv("BPI_TOPIC_NAME"));
        LOG.info(Constants.BPI_TOPIC + ": " + this.parameters.get(Constants.BPI_TOPIC));
        // Get Kafka brokers information
        this.parameters.put(Constants.BROKERS, getKafkaBrokers());
        LOG.info(Constants.BROKERS + ": " + this.parameters.get(Constants.BROKERS));
        String esServiceDnsName;
        try {
            esServiceDnsName = InetAddress.getByName(System.getenv("ES_CS_SERVICE")).getCanonicalHostName();
        } catch (UnknownHostException e) {
            LOG.error("Error trying to get elasticsearch client service DNS name", e);
            throw new RuntimeException(e);
        }
        this.parameters.put(Constants.ES_CS_SERVICE, esServiceDnsName);
        LOG.info(Constants.ES_CS_SERVICE + ": " + this.parameters.get(Constants.ES_CS_SERVICE));
        this.parameters.put(Constants.ES_PORT, System.getenv("ES_PORT"));
        LOG.info(Constants.ES_PORT + ": " + this.parameters.get(Constants.ES_PORT));

        this.parameters.put(Constants.INDEX_NAME, "bitcoin_monitoring");
        this.parameters.put(Constants.BTC_TX_TYPE, "btc_tx");
        this.parameters.put(Constants.BTC_BLK_TYPE, "btc_blk");
        this.parameters.put(Constants.BPI_TYPE, "btc_bpi");
    }

    public static void main( String[] args ) throws UnknownHostException, InvalidTopologyException,
            AuthorizationException, AlreadyAliveException {

        LOG.info("Instanciating Bitcoin Monitoring...");
        BitcoinMonitoring btcm = new BitcoinMonitoring();

        LOG.info("Starting Bitcoin Transactions Monitoring...");
        btcm.bitcoinTopology(args);
        LOG.info("Bitcoin Transactions Monitoring started...");
    }

    private void bitcoinTopology(String[] args) throws InvalidTopologyException, AuthorizationException,
            AlreadyAliveException {


        TopologyBuilder builder = new TopologyBuilder();

        /*
         * Bitcoin Price index Kafka Spout
         */
        LOG.info("Creating Kafka price index spout config builder...");
        KafkaSpoutConfig.Builder<String, String> bpiSpoutConfigBuilder;
        bpiSpoutConfigBuilder = KafkaSpoutConfig.builder(this.parameters.get(Constants.BROKERS),
                this.parameters.get(Constants.BPI_TOPIC));
        bpiSpoutConfigBuilder.setProp("group.id", Constants.BPI_CONSUMERS);
        bpiSpoutConfigBuilder.setFirstPollOffsetStrategy(KafkaSpoutConfig.FirstPollOffsetStrategy.UNCOMMITTED_LATEST);
        bpiSpoutConfigBuilder.setProcessingGuarantee(KafkaSpoutConfig.ProcessingGuarantee.AT_LEAST_ONCE);
        LOG.info("Building Kafka price index spout config builder...");
        KafkaSpoutConfig<String, String> bpiSpoutConfig = bpiSpoutConfigBuilder.build();
        LOG.info("Registering Kafka transaction spout...");
        builder.setSpout(Constants.BPI_SPOUT, new KafkaSpout<>(bpiSpoutConfig), 1).setNumTasks(1);

        /*
         * Bitcoin Transaction Kafka Spout
         */
        LOG.info("Creating Kafka transaction spout config builder...");
        KafkaSpoutConfig.Builder<String, String> txSpoutConfigBuilder;
        txSpoutConfigBuilder = KafkaSpoutConfig.builder(this.parameters.get(Constants.BROKERS),
                this.parameters.get(Constants.BTC_TX_TOPIC));
        txSpoutConfigBuilder.setProp("group.id", Constants.BTC_TX_CONSUMERS);
        txSpoutConfigBuilder.setFirstPollOffsetStrategy(KafkaSpoutConfig.FirstPollOffsetStrategy.UNCOMMITTED_LATEST);
        txSpoutConfigBuilder.setProcessingGuarantee(KafkaSpoutConfig.ProcessingGuarantee.AT_LEAST_ONCE);
        LOG.info("Building Kafka transaction spout config builder...");
        KafkaSpoutConfig<String, String> txSpoutConfig = txSpoutConfigBuilder.build();
        LOG.info("Registering Kafka transaction spout...");
        builder.setSpout(Constants.BTC_TX_SPOUT, new KafkaSpout<>(txSpoutConfig), 5).setNumTasks(5);

        /*
         * Bitcoin Trasactions Parsing Bolt
         */
        LOG.info("Registering BitcoinTransactionsParsingBolt...");
        builder.setBolt(Constants.BTC_TX_PARSING_BOLT, new BitcoinTransactionsParsingBolt(this.parameters), 5).setNumTasks(10)
                .allGrouping(Constants.BPI_SPOUT)
                .shuffleGrouping(Constants.BTC_TX_SPOUT);

        /*
         * Bitcoin Transactions Elasticsearch Index Bolt
         */
        LOG.info("Configuring ES Config");
        EsConfig esConfig = new EsConfig("http://" + this.parameters.get(Constants.ES_CS_SERVICE) + ":" + this.parameters.get(Constants.ES_PORT));
        Header[] headers = {new BasicHeader(HTTP.CONTENT_TYPE, ContentType.APPLICATION_JSON.toString())};
        esConfig.withDefaultHeaders(headers);
        LOG.info("Initializing BTC Tx Elasticsearch tuple mapper");
        EsTupleMapper txEsTupleMapper = new BitcoinTransactionEsTupleMapper();
        EsIndexBolt txEsIndexBolt = new EsIndexBolt(esConfig, txEsTupleMapper);
        LOG.info("Registering " + Constants.BTC_TX_ES_BOLT);
        builder.setBolt(Constants.BTC_TX_ES_BOLT, txEsIndexBolt, 5).setNumTasks(10).shuffleGrouping(Constants.BTC_TX_PARSING_BOLT);

        /*
         * Bitcoin Price Index Parsing Bolt
         */
        LOG.info("Registering BitcoinPriceIndexParsingBolt...");
        builder.setBolt(Constants.BPI_PARSING_BOLT, new BitcoinPriceIndexParsingBolt(this.parameters), 1).setNumTasks(1)
            .shuffleGrouping(Constants.BPI_SPOUT);

        /*
         * Bitcoin Price Index Elasticsearch Index Bolt
         */
        LOG.info("Initializing BPI Elasticsearch tuple mapper");
        EsTupleMapper bpiEsTupleMapper = new BitcoinPriceIndexEsTupleMapper();
        EsIndexBolt bpiEsIndexBolt = new EsIndexBolt(esConfig, bpiEsTupleMapper);
        LOG.info("Registering " + Constants.BPI_ES_BOLT);
        builder.setBolt(Constants.BPI_ES_BOLT, bpiEsIndexBolt, 1).setNumTasks(1).shuffleGrouping(Constants.BPI_PARSING_BOLT);

        /*
         * Bitcoin Block Kafka Spout
         */
        LOG.info("Creating Kafka block spout config builder...");
        KafkaSpoutConfig.Builder<String, String> blkSpoutConfigBuilder;
        blkSpoutConfigBuilder = KafkaSpoutConfig.builder(this.parameters.get(Constants.BROKERS),
                this.parameters.get(Constants.BTC_BLK_TOPIC));
        blkSpoutConfigBuilder.setProp("group.id", Constants.BTC_BLK_CONSUMERS);
        blkSpoutConfigBuilder.setFirstPollOffsetStrategy(KafkaSpoutConfig.FirstPollOffsetStrategy.UNCOMMITTED_LATEST);
        blkSpoutConfigBuilder.setProcessingGuarantee(KafkaSpoutConfig.ProcessingGuarantee.AT_LEAST_ONCE);
        LOG.info("Building Kafka block spout config builder...");
        KafkaSpoutConfig<String, String> blkSpoutConfig = blkSpoutConfigBuilder.build();
        LOG.info("Registering Kafka block spout...");
        builder.setSpout(Constants.BTC_BLK_SPOUT, new KafkaSpout<>(blkSpoutConfig), 1).setNumTasks(1);

        /*
         * Bitcoin Block Parsing Bolt
         */
        LOG.info("Registering BitcoinBlockParsingBolt...");
        builder.setBolt(Constants.BTC_BLK_PARSING_BOLT, new BitcoinBlockParsingBolt(this.parameters), 1).setNumTasks(1)
                .allGrouping(Constants.BPI_SPOUT)
                .shuffleGrouping(Constants.BTC_BLK_SPOUT);

        /*
         * Bitcoin Block Elasticsearch Index Bolt
         */
        LOG.info("Initializing Block Elasticsearch tuple mapper");
        EsTupleMapper blkEsTupleMapper = new BitcoinBlockEsTupleMapper();
        EsIndexBolt blkEsIndexBolt = new EsIndexBolt(esConfig, blkEsTupleMapper);
        LOG.info("Registering " + Constants.BTC_BLK_ES_BOLT);
        builder.setBolt(Constants.BTC_BLK_ES_BOLT, blkEsIndexBolt, 1).setNumTasks(1).shuffleGrouping(Constants.BTC_BLK_PARSING_BOLT);

        StormTopology topology = builder.createTopology();

        Config config = new Config();
        config.setMessageTimeoutSecs(60*30);
        config.setNumWorkers(3);
        String topolotyName = "Bitcoin-Transactions";

        if (args.length > 0 && args[0].equals("remote")) {
            LOG.info("Submitting Bitcoin Transactions topology to remote cluster...");
            StormSubmitter.submitTopology(topolotyName, config, topology);
        } else {
            LOG.info("Submitting Bitcoin Transactions topology to local cluster...");
            LocalCluster cluster = new LocalCluster();
            cluster.submitTopology(topolotyName, config, topology);
        }
    }

    private String getKafkaBrokers() throws UnknownHostException {
        LOG.info("Retrieving brokers list");
        StringBuilder brokers = new StringBuilder();
        InetAddress[] brokersAddresses = InetAddress.getAllByName(System.getenv("KAFKA_HS_SERVICE"));
        for (int i=0; i<brokersAddresses.length; i++) {
            brokers.append(brokersAddresses[i].getCanonicalHostName()).append(":")
                    .append(System.getenv("KAFKA_BROKER_PORT"));
            if (i != (brokersAddresses.length - 1)) brokers.append(",");
        }
        return brokers.toString();
    }
}
