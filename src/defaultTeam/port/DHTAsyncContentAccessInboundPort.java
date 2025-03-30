package defaultTeam.port;
import defaultTeam.AsyncNodeComponent;
import fr.sorbonne_u.components.ComponentI;
import fr.sorbonne_u.components.endpoints.EndPointI;
import fr.sorbonne_u.cps.dht_mapreduce.interfaces.content.ContentAccessCI;
import fr.sorbonne_u.cps.dht_mapreduce.interfaces.content.ContentDataI;
import fr.sorbonne_u.cps.dht_mapreduce.interfaces.content.ContentKeyI;
import fr.sorbonne_u.cps.dht_mapreduce.interfaces.content.ResultReceptionCI;

public class DHTAsyncContentAccessInboundPort extends DHTContentAccessInboundPort implements ContentAccessCI {
    private static final long serialVersionUID = 1L;

    public DHTAsyncContentAccessInboundPort(String uri, ComponentI owner) throws Exception {
        super(uri, owner);
    }

	@Override
	public <I extends ResultReceptionCI> void get(String computationURI, ContentKeyI key, EndPointI<I> caller)
			throws Exception {

	    try {
	        ((AsyncNodeComponent) owner).get(computationURI, key, caller);
	    } catch (Exception e) {
	        e.printStackTrace();
	    }
	}

	@Override
	public <I extends ResultReceptionCI> void put(String computationURI, ContentKeyI key, ContentDataI value,
			EndPointI<I> caller) throws Exception {
        try {
            ((AsyncNodeComponent) owner).put(computationURI, key, value, caller);
        } catch (Exception e) {
            e.printStackTrace();
        }
		
	}

	@Override
	public <I extends ResultReceptionCI> void remove(String computationURI, ContentKeyI key, EndPointI<I> caller)
			throws Exception {
        try {
            ((AsyncNodeComponent) owner).remove(computationURI, key, caller);
        } catch (Exception e) {
            e.printStackTrace();
        }	
	}

}