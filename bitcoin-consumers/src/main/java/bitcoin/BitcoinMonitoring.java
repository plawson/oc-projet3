package bitcoin;

import org.apache.storm.Config;
import org.apache.storm.LocalCluster;
import org.apache.storm.StormSubmitter;
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
        this.parameters.put("btc-tx-topic", System.getenv("BTC_TX_TOPIC_NAME"));
        LOG.info("btc-tx-topic: " + this.parameters.get("btc-tx-topic"));
        this.parameters.put("btc-blk-topic", System.getenv("BTC_BLK_TOPIC_NAME"));
        LOG.info("btc-blk-topic: " + this.parameters.get("btc-blk-topic"));
        this.parameters.put("bpi-topic", System.getenv("BPI_TOPIC_NAME"));
        LOG.info("bpi-topic: " + this.parameters.get("bpi-topic"));
        // Get Kafka brokers information
        this.parameters.put("brokers", getKafkaBrokers());
        LOG.info("bootstrap_servers: " + this.parameters.get("brokers"));
        String esServiceDnsName;
        try {
            esServiceDnsName = InetAddress.getByName(System.getenv("ES_CS_SERVICE")).getCanonicalHostName();
        } catch (UnknownHostException e) {
            LOG.error("Error trying to get elasticsearch client service DNS name", e);
            throw new RuntimeException(e);
        }
        this.parameters.put("es-cs-service", esServiceDnsName);
        LOG.info("es-cs-service: " + this.parameters.get("es-cs-service"));
        this.parameters.put("es-port", System.getenv("ES_PORT"));
        LOG.info("es-port: " + this.parameters.get("es-port"));
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
        spoutConfigBuilder = KafkaSpoutConfig.builder(this.parameters.get("brokers"),
                this.parameters.get("btc-tx-topic"));
        spoutConfigBuilder.setGroupId("btc-tx-consumers");
        LOG.info("Building Kafka spout config builder...");
        KafkaSpoutConfig<String, String> spoutConfig = spoutConfigBuilder.build();
        LOG.info("Registering Kafka spout...");
        builder.setSpout("btc-tx-spout", new KafkaSpout<>(spoutConfig), 5).setNumTasks(5);
        LOG.info("Registering BitcoinTransactionsBolt...");
        builder.setBolt("btc-tx-bolt", new BitcoinTransactionsBolt(this.parameters.get("es-cs-service"),
                this.parameters.get("es-port")), 5).setNumTasks(10)
                .shuffleGrouping("btc-tx-spout");

        StormTopology topology = builder.createTopology();

        Config config = new Config();
        config.setMessageTimeoutSecs(60*30);
        config.setNumWorkers(3);
        // String topolotyName = "Bitcoin_Transactions_" + System.currentTimeMillis();
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
