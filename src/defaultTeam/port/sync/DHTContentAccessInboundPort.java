package defaultTeam.port.sync;

import defaultTeam.NodeAsyncComponent;
import defaultTeam.old.NodeComponent;
import fr.sorbonne_u.components.ComponentI;
import fr.sorbonne_u.components.ports.AbstractInboundPort;
import fr.sorbonne_u.cps.dht_mapreduce.interfaces.content.ContentAccessSyncCI;
import fr.sorbonne_u.cps.dht_mapreduce.interfaces.content.ContentDataI;
import fr.sorbonne_u.cps.dht_mapreduce.interfaces.content.ContentKeyI;

public class DHTContentAccessInboundPort extends AbstractInboundPort implements ContentAccessSyncCI{
    private static final long serialVersionUID = 1L;

    public DHTContentAccessInboundPort(String uri, ComponentI owner) throws Exception {
        super(uri, ContentAccessSyncCI.class, (ComponentI) owner);
    }

    @Override
	public ContentDataI getSync(String computationURI, ContentKeyI key) throws Exception {
		return ((NodeAsyncComponent) this.owner).getSync(computationURI, key);
	}

	@Override
	public ContentDataI putSync(
		String computationURI, 
		ContentKeyI key, 
		ContentDataI value) throws Exception {
		
		return ((NodeAsyncComponent) this.owner).putSync(computationURI, key, value);
	}

	@Override
	public ContentDataI removeSync(String computationURI, ContentKeyI key) throws Exception {
		return ((NodeAsyncComponent) this.owner).removeSync(computationURI, key);
	}

	@Override
	public void clearComputation(String computationURI) throws Exception {
		((NodeAsyncComponent) this.owner).clearComputation(computationURI);
	}

    
}