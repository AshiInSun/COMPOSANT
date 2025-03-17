package defaultTeam.port;

import fr.sorbonne_u.components.endpoints.EndPointI;
import fr.sorbonne_u.cps.dht_mapreduce.interfaces.content.ContentAccessCI;
import fr.sorbonne_u.cps.dht_mapreduce.interfaces.content.ContentDataI;
import fr.sorbonne_u.cps.dht_mapreduce.interfaces.content.ContentKeyI;
import fr.sorbonne_u.cps.dht_mapreduce.interfaces.content.ResultReceptionCI;

public class DHTAsyncContentAccessConnector extends DHTContentAccessConnector implements ContentAccessCI {

    @Override
    public <I extends ResultReceptionCI> void get(String computationURI, ContentKeyI key, EndPointI<I> caller)
            throws Exception {
        ((ContentAccessCI) this.offering).get(computationURI, key, caller);
    }

    @Override
    public <I extends ResultReceptionCI> void put(String computationURI, ContentKeyI key, ContentDataI value,
                                                  EndPointI<I> caller) throws Exception {
        ((ContentAccessCI) this.offering).put(computationURI, key, value, caller);
    }

    @Override
    public <I extends ResultReceptionCI> void remove(String computationURI, ContentKeyI key, EndPointI<I> caller)
            throws Exception {
        ((ContentAccessCI) this.offering).remove(computationURI, key, caller);
    }
}