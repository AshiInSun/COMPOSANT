package defaultTeam.port.sync;

import fr.sorbonne_u.components.connectors.AbstractConnector;
import fr.sorbonne_u.cps.dht_mapreduce.interfaces.content.ContentAccessSyncCI;
import fr.sorbonne_u.cps.dht_mapreduce.interfaces.content.ContentDataI;
import fr.sorbonne_u.cps.dht_mapreduce.interfaces.content.ContentKeyI;

public class DHTContentAccessConnector extends AbstractConnector implements ContentAccessSyncCI {

	@Override
	public ContentDataI getSync(String computationURI, ContentKeyI key) throws Exception {
		return ((ContentAccessSyncCI) this.offering).getSync(computationURI, key);
	}

	@Override
	public ContentDataI putSync(
			String computationURI, 
			ContentKeyI key, 
			ContentDataI value) throws Exception {
		
		return ((ContentAccessSyncCI) this.offering).putSync(computationURI, key, value);
	}

	@Override
	public ContentDataI removeSync(String computationURI, ContentKeyI key) throws Exception {
		return ((ContentAccessSyncCI) this.offering).removeSync(computationURI, key);
	}

	@Override
	public void clearComputation(String computationURI) throws Exception {
		((ContentAccessSyncCI) this.offering).clearComputation(computationURI);
	}

	
    
}