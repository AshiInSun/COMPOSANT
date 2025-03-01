package defaultTeam.port;

import fr.sorbonne_u.components.ComponentI;
import fr.sorbonne_u.components.ports.AbstractInboundPort;
import fr.sorbonne_u.cps.dht_mapreduce.interfaces.content.ContentAccessSyncCI;
import fr.sorbonne_u.cps.dht_mapreduce.interfaces.content.ContentDataI;
import fr.sorbonne_u.cps.dht_mapreduce.interfaces.content.ContentKeyI;
import fr.sorbonne_u.cps.dht_mapreduce.interfaces.frontend.DHTServicesCI;

public class DHTMapReduceInboundPort extends AbstractInboundPort implements ContentAccessSyncCI{
    private static final long serialVersionUID = 1L;
    private final ComponentI owner;

    public DHTMapReduceInboundPort(String uri, ComponentI owner) throws Exception {
        super(uri, DHTServicesCI.class, (ComponentI) owner);
        this.owner = owner;
    }

	@Override
	public ContentDataI getSync(String computationURI, ContentKeyI key) throws Exception {
		return ((ContentAccessSyncCI) this.owner).getSync(computationURI, key);
	}

	@Override
	public ContentDataI putSync(
		String computationURI, 
		ContentKeyI key, 
		ContentDataI value) throws Exception {
		
		return ((ContentAccessSyncCI) this.owner).putSync(computationURI, key, value);
	}

	@Override
	public ContentDataI removeSync(String computationURI, ContentKeyI key) throws Exception {
		return ((ContentAccessSyncCI) this.owner).removeSync(computationURI, key);
	}

	@Override
	public void clearComputation(String computationURI) throws Exception {
		((ContentAccessSyncCI) this.owner).clearComputation(computationURI);
	}

	

    
}