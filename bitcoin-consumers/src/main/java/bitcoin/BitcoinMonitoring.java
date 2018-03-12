package bitcoin;

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
        this.parameters.put(Constants.ES_CLUSTER_NAME, System.getenv("ES_CLUSTER_NAME"));
        LOG.info(Constants.ES_CLUSTER_NAME + ": " + this.parameters.get(Constants.ES_CLUSTER_NAME));

        this.parameters.put(Constants.INDEX_NAME, "bitcoin_monitoring");
        this.parameters.put(Constants.BTC_TX_TYPE, "btc_tx");
    }

    public static void main( String[] args ) throws UnknownHostException, InvalidTopologyException,
            AuthorizationException, AlreadyAliveException {

        LOG.info("Instanciating Bitcoin Monitoring...");
        BitcoinMonitoring btcm = new BitcoinMonitoring();

        LOG.info("Starting Bitcoin Transactions Monitoring...");
        btcm.bitcoinTransactionTopology(args);
        LOG.info("Bitcoin Transactions Monitoring started...");
    }

    private void bitcoinTransactionTopology(String[] args) throws InvalidTopologyException, AuthorizationException,
            AlreadyAliveException {


        TopologyBuilder builder = new TopologyBuilder();

        LOG.info("Creating Kafka spout config builder...");
        KafkaSpoutConfig.Builder<String, String> spoutConfigBuilder;
        spoutConfigBuilder = KafkaSpoutConfig.builder(this.parameters.get(Constants.BROKERS),
                this.parameters.get(Constants.BTC_TX_TOPIC));
        spoutConfigBuilder.setProp("group.id", Constants.BTC_TX_CONSUMERS);
        spoutConfigBuilder.setFirstPollOffsetStrategy(KafkaSpoutConfig.FirstPollOffsetStrategy.UNCOMMITTED_LATEST);
        spoutConfigBuilder.setProcessingGuarantee(KafkaSpoutConfig.ProcessingGuarantee.AT_LEAST_ONCE);

        LOG.info("Building Kafka spout config builder...");
        KafkaSpoutConfig<String, String> spoutConfig = spoutConfigBuilder.build();
        LOG.info("Registering Kafka spout...");
        builder.setSpout(Constants.BTC_TX_SPOUT, new KafkaSpout<>(spoutConfig), 5).setNumTasks(5);
        LOG.info("Registering BitcoinTransactionsParsingBolt...");
        builder.setBolt(Constants.BTC_TX_PARSING_BOLT, new BitcoinTransactionsParsingBolt(this.parameters), 5).setNumTasks(10)
                .shuffleGrouping(Constants.BTC_TX_SPOUT);

        LOG.info("Configuring " + Constants.BTC_TX_ES_BOLT);
        EsConfig esConfig = new EsConfig("http://" + this.parameters.get(Constants.ES_CS_SERVICE) + ":" + this.parameters.get(Constants.ES_PORT));
        LOG.info("Initializing BTC Tx Elasticsearch tuple mapper");
        EsTupleMapper esTupleMapper = new BitcoinTransactionEsTupleMapper();
        EsIndexBolt esIndexBolt = new EsIndexBolt(esConfig, esTupleMapper);
        LOG.info("Registering " + Constants.BTC_TX_ES_BOLT);
        builder.setBolt(Constants.BTC_TX_ES_BOLT, esIndexBolt, 5).setNumTasks(10).shuffleGrouping(Constants.BTC_TX_PARSING_BOLT);

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
