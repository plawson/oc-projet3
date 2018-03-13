package bitcoin.price;

import bitcoin.common.Constants;
import org.apache.storm.elasticsearch.common.EsTupleMapper;
import org.apache.storm.tuple.ITuple;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.Map;

public class BitcoinPriceIndexEsTupleMapper implements EsTupleMapper {

    private static final Logger LOG = LoggerFactory.getLogger(BitcoinPriceIndexEsTupleMapper.class);

    @Override
    public String getSource(ITuple tuple) {

        LOG.debug("Source: " + tuple.getStringByField(Constants.SOURCE));
        return tuple.getStringByField(Constants.SOURCE);
    }

    @Override
    public String getIndex(ITuple tuple) {
        LOG.debug("Index: " + tuple.getStringByField(Constants.BPI_FIELD_INDEX));
        return tuple.getStringByField(Constants.BPI_FIELD_INDEX);
    }

    @Override
    public String getType(ITuple tuple) {
        LOG.debug("Type: " + tuple.getStringByField(Constants.BPI_FIELD_TYPE));
        return tuple.getStringByField(Constants.BPI_FIELD_TYPE);
    }

    @Override
    public String getId(ITuple tuple) {
        LOG.debug("Id: " + tuple.getStringByField(Constants.BPI_FIELD_ID));
        return tuple.getStringByField(Constants.BPI_FIELD_ID);
    }

    @Override
    public Map<String, String> getParams(ITuple tuple, Map<String, String> map) {
        map.put(Constants.OP_TYPE, Constants.OP_TYPE_CREATE);
        return map;
    }
}
