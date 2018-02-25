package bitcoin;

import org.apache.storm.elasticsearch.common.EsTupleMapper;
import org.apache.storm.tuple.ITuple;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

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
}