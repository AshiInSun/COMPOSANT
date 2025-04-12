package defaultTeam.port;
import defaultTeam.NodeAsyncComponent;
import defaultTeam.port.sync.DHTContentAccessInboundPort;
import fr.sorbonne_u.components.ComponentI;
import fr.sorbonne_u.components.endpoints.EndPointI;
import fr.sorbonne_u.cps.dht_mapreduce.interfaces.content.ContentAccessCI;
import fr.sorbonne_u.cps.dht_mapreduce.interfaces.content.ContentDataI;
import fr.sorbonne_u.cps.dht_mapreduce.interfaces.content.ContentKeyI;
import fr.sorbonne_u.cps.dht_mapreduce.interfaces.content.ResultReceptionCI;

public class DHTAsyncContentAccessInboundPort extends DHTContentAccessInboundPort implements ContentAccessCI {
    private static final long serialVersionUID = 1L;
    private static final String CONTENT_ACCESS_HANDLER_URI = "caah";

    public DHTAsyncContentAccessInboundPort(String uri, ComponentI owner) throws Exception {
        super(uri, owner);
    }

	@Override
	public <I extends ResultReceptionCI> void get(String computationURI, ContentKeyI key, EndPointI<I> caller)
			throws Exception {
		this.getOwner().runTask(CONTENT_ACCESS_HANDLER_URI, o -> {
		    try {
		        ((NodeAsyncComponent) o).get(computationURI, key, caller);
		    } catch (Exception e) {
		        e.printStackTrace();
		    }
		});
	}

	@Override
	public <I extends ResultReceptionCI> void put(String computationURI, ContentKeyI key, ContentDataI value,
			EndPointI<I> caller) throws Exception {
		this.getOwner().runTask(CONTENT_ACCESS_HANDLER_URI, o -> {
	        try {
	            ((NodeAsyncComponent) o).put(computationURI, key, value, caller);
	        } catch (Exception e) {
	            e.printStackTrace();
	        }
		});
		
	}

	@Override
	public <I extends ResultReceptionCI> void remove(String computationURI, ContentKeyI key, EndPointI<I> caller)
			throws Exception {
		this.getOwner().runTask(CONTENT_ACCESS_HANDLER_URI, o -> {
	        try {
	            ((NodeAsyncComponent) o).remove(computationURI, key, caller);
	        } catch (Exception e) {
	            e.printStackTrace();
	        }	
		});
	}

}