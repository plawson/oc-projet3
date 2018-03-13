package bitcoin.common;

/**
 * Created by plawson on 25/02/2018.
 *
 */
public class Constants {

    public static final String INDEX_NAME = "index-name";
    public static final String BTC_TX_TYPE = "btc-tx-type";
    public static final String BTC_BLK_TYPE = "btc-blk-type";
    public static final String BPI_TYPE = "bpi-type";

    public static final String BTC_TX_TOPIC = "btc-tx-topic";
    public static final String BTC_BLK_TOPIC = "btc-blk-topic";
    public static final String BPI_TOPIC = "bpi-topic";
    public static final String BROKERS = "brokers";
    public static final String ES_CS_SERVICE = "es-cs-service";
    public static final String ES_PORT = "es-port";
    // public static final String ES_CLUSTER_NAME = "es-cluster-name";

    public static final String BTC_TX_CONSUMERS = "btc-tx-consumers";
    public static final String BTC_TX_SPOUT = "btc-tx-spout";
    public static final String BTC_TX_PARSING_BOLT = "btc-tx-parsing-bolt";
    public static final String BTC_TX_ES_BOLT = "btc-tx-es-bolt";

    public static final String BTC_BLK_CONSUMERS = "btc-blk-consumers";
    public static final String BTC_BLK_SPOUT = "btc-blk-spout";
    public static final String BTC_BLK_PARSING_BOLT = "btc-blk-parsing-bolt";
    public static final String BTC_BLK_ES_BOLT = "btc-blk-es-bolt";

    public static final String BPI_CONSUMERS = "bpi-consumers";
    public static final String BPI_SPOUT = "bpi-spout";
    public static final String BPI_PARSING_BOLT = "bpi-parsing-bolt";
    public static final String BPI_ES_BOLT = "bpi-es-bolt";

    public static final String BTC_TX_FIELD_TX_ID = "tx_id";
    public static final String BTC_TX_FIELD_DATE = "btc_timestamp";
    public static final String BTC_TX_FIELD_TX_BTC_AMOUNT = "tx_btc_amount";
    public static final String BTC_TX_FIELD_TX_EUR_AMOUNT = "tx_eur_amount";
    public static final String BTC_TX_FIELD_INDEX = "tx_index";
    public static final String BTC_TX_FIELD_TYPE = "tx_type";

    public static final String BTC_BLK_FIELD_TX_ID = "tx_id";
    public static final String BTC_BLK_FIELD_DATE = "btc_timestamp";
    public static final String BTC_BLK_FIELD_BTC_REWARD = "blk_btc_reward";
    public static final String BTC_BLK_FIELD_EUR_REWARD = "blk_eur_reward";
    public static final String BTC_BLK_FIELD_OWNER = "blk_owner";
    public static final String BTC_BLK_FIELD_INDEX = "blk_index";
    public static final String BTC_BLK_FIELD_TYPE = "blk_type";

    public static final String BPI_FIELD_INDEX = "bpi_index";
    public static final String BPI_FIELD_TYPE = "bpi_type";
    public static final String BPI_FIELD_ID = "bpi_id";
    public static final String BPI_FIELD_DATE = "btc_timestamp";
    public static final String BPI_FIELD_RATE = "bpi_rate";
    public static final String BPI_FIELD_CURRENCY = "bpi_currency";

    public static final String SOURCE = "source";

    public static final String OP_TYPE = "op_type";
    public static final String OP_TYPE_CREATE = "create";
}
