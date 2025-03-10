package defaultTeam.port;

import java.io.Serializable;

import fr.sorbonne_u.components.ComponentI;
import fr.sorbonne_u.components.ports.AbstractOutboundPort;
import fr.sorbonne_u.cps.dht_mapreduce.interfaces.mapreduce.CombinatorI;
import fr.sorbonne_u.cps.dht_mapreduce.interfaces.mapreduce.MapReduceSyncCI;
import fr.sorbonne_u.cps.dht_mapreduce.interfaces.mapreduce.ProcessorI;
import fr.sorbonne_u.cps.dht_mapreduce.interfaces.mapreduce.ReductorI;
import fr.sorbonne_u.cps.dht_mapreduce.interfaces.mapreduce.SelectorI;

public class DHTMapReduceOutboundPort extends AbstractOutboundPort implements MapReduceSyncCI {
    private static final long serialVersionUID = 1L;

    public DHTMapReduceOutboundPort(String uri, ComponentI owner) throws Exception {
        super(uri, MapReduceSyncCI.class, owner);
    }
    
    public DHTMapReduceOutboundPort(ComponentI owner) throws Exception {
        super(MapReduceSyncCI.class, owner);
    }

	@Override
	public <R extends Serializable> void mapSync(
		String computationURI, 
		SelectorI selector, 
		ProcessorI<R> processor) throws Exception {
		
		((MapReduceSyncCI) this.getConnector()).mapSync(computationURI, selector, processor);
	}

	@Override
	public <A extends Serializable, R> A reduceSync(
			String computationURI, 
			ReductorI<A, R> reductor,
			CombinatorI<A> combinator, 
			A currentAcc) throws Exception {
		
		return ((MapReduceSyncCI) this.getConnector()).reduceSync(computationURI, reductor, combinator, currentAcc);
	}

	@Override
	public void clearMapReduceComputation(String computationURI) throws Exception {
		((MapReduceSyncCI) this.getConnector()).clearMapReduceComputation(computationURI);
	}
	
}