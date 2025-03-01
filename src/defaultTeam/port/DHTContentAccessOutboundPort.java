package defaultTeam.port;

import fr.sorbonne_u.components.ComponentI;
import fr.sorbonne_u.components.ports.AbstractOutboundPort;
import fr.sorbonne_u.cps.dht_mapreduce.interfaces.content.ContentAccessSyncCI;
import fr.sorbonne_u.cps.dht_mapreduce.interfaces.content.ContentDataI;
import fr.sorbonne_u.cps.dht_mapreduce.interfaces.content.ContentKeyI;
import fr.sorbonne_u.cps.dht_mapreduce.interfaces.frontend.DHTServicesCI;

public class DHTContentAccessOutboundPort extends AbstractOutboundPort implements ContentAccessSyncCI {
    private static final long serialVersionUID = 1L;

    public DHTContentAccessOutboundPort(String uri, ComponentI owner) throws Exception {
        super(uri, DHTServicesCI.class, owner);
    }

	@Override
	public ContentDataI getSync(String computationURI, ContentKeyI key) throws Exception {
		return ((ContentAccessSyncCI) this.getConnector()).getSync(computationURI, key);
	}

	@Override
	public ContentDataI putSync(
			String computationURI, 
			ContentKeyI key, 
			ContentDataI value) throws Exception {
		
		return ((ContentAccessSyncCI) this.getConnector()).putSync(computationURI, key, value);
	}

	@Override
	public ContentDataI removeSync(String computationURI, ContentKeyI key) throws Exception {
		return ((ContentAccessSyncCI) this.getConnector()).removeSync(computationURI, key);
	}

	@Override
	public void clearComputation(String computationURI) throws Exception {
		((ContentAccessSyncCI) this.getConnector()).clearComputation(computationURI);
	}


	
}