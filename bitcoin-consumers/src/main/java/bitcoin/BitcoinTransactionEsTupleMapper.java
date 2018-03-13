package bitcoin;

import org.apache.storm.elasticsearch.common.EsTupleMapper;
import org.apache.storm.tuple.ITuple;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.HashMap;
import java.util.Map;

/**
 * Created by plawson on 25/02/2018.
 *
 */
public class BitcoinTransactionEsTupleMapper implements EsTupleMapper {


    private static final Logger LOG = LoggerFactory.getLogger(BitcoinTransactionEsTupleMapper.class);

    @Override
    public String getSource(ITuple tuple) {
        LOG.debug("Source: " + tuple.getStringByField(Constants.SOURCE));
        return tuple.getStringByField(Constants.SOURCE);
    }

    @Override
    public String getIndex(ITuple tuple) {
        LOG.debug("Source: " + tuple.getStringByField(Constants.BTC_TX_FIELD_INDEX));
        return tuple.getStringByField(Constants.BTC_TX_FIELD_INDEX);
    }

    @Override
    public String getType(ITuple tuple) {
        LOG.debug("Source: " + tuple.getStringByField(Constants.BTC_TX_FIELD_TYPE));
        return tuple.getStringByField(Constants.BTC_TX_FIELD_TYPE);
    }

    @Override
    public String getId(ITuple tuple) {
        LOG.debug("Source: " + tuple.getStringByField(Constants.BTC_TX_FIELD_TX_ID));
        return tuple.getStringByField(Constants.BTC_TX_FIELD_TX_ID);
    }

    @Override
    public Map<String, String> getParams(ITuple iTuple, Map<String, String> map) {
        // Force create operation and fails if BTC tx_id already exists
        Map<String, String> params = new HashMap<>();
        params.put(Constants.OP_TYPE, Constants.OP_TYPE_CREATE);
        return params;
    }
}
