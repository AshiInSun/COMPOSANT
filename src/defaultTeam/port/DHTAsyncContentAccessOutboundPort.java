package defaultTeam.port;


import defaultTeam.port.sync.DHTContentAccessOutboundPort;
import fr.sorbonne_u.components.ComponentI;
import fr.sorbonne_u.components.endpoints.EndPointI;
import fr.sorbonne_u.cps.dht_mapreduce.interfaces.content.ContentAccessCI;
import fr.sorbonne_u.cps.dht_mapreduce.interfaces.content.ContentDataI;
import fr.sorbonne_u.cps.dht_mapreduce.interfaces.content.ContentKeyI;
import fr.sorbonne_u.cps.dht_mapreduce.interfaces.content.ResultReceptionCI;

public class DHTAsyncContentAccessOutboundPort extends DHTContentAccessOutboundPort implements ContentAccessCI {
    private static final long serialVersionUID = 1L;

    public DHTAsyncContentAccessOutboundPort(String uri, ComponentI owner) throws Exception {
        super(uri, owner);
    }

    @Override
    public <I extends ResultReceptionCI> void get(String computationURI, ContentKeyI key, EndPointI<I> caller)
            throws Exception {
        ((ContentAccessCI) this.getConnector()).get(computationURI, key, caller);
    }

    @Override
    public <I extends ResultReceptionCI> void put(String computationURI, ContentKeyI key, ContentDataI value,
                                                  EndPointI<I> caller) throws Exception {
        ((ContentAccessCI) this.getConnector()).put(computationURI, key, value, caller);
    }

    @Override
    public <I extends ResultReceptionCI> void remove(String computationURI, ContentKeyI key, EndPointI<I> caller)
            throws Exception {
        ((ContentAccessCI) this.getConnector()).remove(computationURI, key, caller);
    }
}