import java.io.Serializable;
import java.util.AbstractMap;
import java.util.Map;
import java.util.stream.Collectors;

import fr.sorbonne_u.components.ComponentI;
import fr.sorbonne_u.components.ports.AbstractInboundPort;
import fr.sorbonne_u.cps.dht_mapreduce.interfaces.content.ContentAccessSyncCI;
import fr.sorbonne_u.cps.dht_mapreduce.interfaces.content.ContentDataI;
import fr.sorbonne_u.cps.dht_mapreduce.interfaces.content.ContentKeyI;
import fr.sorbonne_u.cps.dht_mapreduce.interfaces.frontend.DHTServicesCI;
import fr.sorbonne_u.cps.dht_mapreduce.interfaces.mapreduce.CombinatorI;
import fr.sorbonne_u.cps.dht_mapreduce.interfaces.mapreduce.MapReduceSyncCI;
import fr.sorbonne_u.cps.dht_mapreduce.interfaces.mapreduce.ProcessorI;
import fr.sorbonne_u.cps.dht_mapreduce.interfaces.mapreduce.ReductorI;
import fr.sorbonne_u.cps.dht_mapreduce.interfaces.mapreduce.SelectorI;

class DHTInboundPort extends AbstractInboundPort implements ContentAccessSyncCI, MapReduceSyncCI {
    private static final long serialVersionUID = 1L;
    private final ComponentI owner;

    protected DHTInboundPort(String uri, ComponentI owner) throws Exception {
        super(uri, DHTServicesCI.class, (ComponentI) owner);
        this.owner = owner;
    }

    @Override
	public ContentDataI getSync(String computationURI, ContentKeyI key) throws Exception {	
    	if (this.owner instanceof ContentAccessSyncCI) {
            return ((ContentAccessSyncCI) this.owner).getSync(computationURI, key);
        }
        throw new Exception("Composant non compatible avec ContentAccessSyncCI");
	}

	@Override
	public ContentDataI putSync(String computationURI, ContentKeyI key, ContentDataI value) throws Exception {
		if (this.owner instanceof ContentAccessSyncCI) {
            return ((ContentAccessSyncCI) this.owner).putSync(computationURI, key, value);
        }
        throw new Exception("Composant non compatible avec ContentAccessSyncCI");
    }

	@Override
	public ContentDataI removeSync(String computationURI, ContentKeyI key) throws Exception {
		if (this.owner instanceof ContentAccessSyncCI) {
            return ((ContentAccessSyncCI) this.owner).removeSync(computationURI, key);
        }
        throw new Exception("Composant non compatible avec ContentAccessSyncCI");
	}
	
	@Override
	public void clearMapReduceComputation(String computationURI) throws Exception {
		if (this.owner instanceof MapReduceSyncCI) {
			((MapReduceSyncCI) this.owner).clearMapReduceComputation(computationURI);
        }
        throw new Exception("Composant non compatible avec MapReduceSyncCI");
	}

	@Override
	public void clearComputation(String computationURI) throws Exception {
		if (this.owner instanceof ContentAccessSyncCI) {
            ((ContentAccessSyncCI) this.owner).clearComputation(computationURI);
        }
        throw new Exception("Composant non compatible avec ContentAccessSyncCI");
	}
	
	@Override
	public <R extends Serializable> void mapSync(String computationURI, SelectorI selector, 
												 ProcessorI<R> processor) throws Exception {
		if (this.owner instanceof MapReduceSyncCI) {
			((MapReduceSyncCI) this.owner).mapSync(computationURI, selector, processor);
        }
        throw new Exception("Composant non compatible avec MapReduceSyncCI");
	}

	@Override
	public <A extends Serializable, R> A reduceSync(String computationURI, ReductorI<A, R> reductor, 
													CombinatorI<A> combinator, A currentAcc)
			throws Exception {
		if (this.owner instanceof MapReduceSyncCI) {
			return ((MapReduceSyncCI) this.owner).reduceSync(computationURI, reductor, combinator, currentAcc);
        }
        throw new Exception("Composant non compatible avec MapReduceSyncCI");
		
	}
}