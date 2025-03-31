package defaultTeam.port.sync;

import java.io.Serializable;

import defaultTeam.old.NodeComponent;
import fr.sorbonne_u.components.ComponentI;
import fr.sorbonne_u.components.ports.AbstractInboundPort;
import fr.sorbonne_u.cps.dht_mapreduce.interfaces.mapreduce.CombinatorI;
import fr.sorbonne_u.cps.dht_mapreduce.interfaces.mapreduce.MapReduceSyncCI;
import fr.sorbonne_u.cps.dht_mapreduce.interfaces.mapreduce.ProcessorI;
import fr.sorbonne_u.cps.dht_mapreduce.interfaces.mapreduce.ReductorI;
import fr.sorbonne_u.cps.dht_mapreduce.interfaces.mapreduce.SelectorI;

public class DHTMapReduceInboundPort extends AbstractInboundPort implements MapReduceSyncCI{
    private static final long serialVersionUID = 1L;

    public DHTMapReduceInboundPort(String uri, ComponentI owner) throws Exception {
        super(uri, MapReduceSyncCI.class, (ComponentI) owner);
    }
    
    @Override
	public <R extends Serializable> void mapSync(
		String computationURI, 
		SelectorI selector, 
		ProcessorI<R> processor) throws Exception {
		
		((NodeComponent) this.owner).mapSync(computationURI, selector, processor);
	}

	@Override
	public <A extends Serializable, R> A reduceSync(
		String computationURI, 
		ReductorI<A, R> reductor,
		CombinatorI<A> combinator,
		A currentAcc) throws Exception {
		
		return ((NodeComponent) this.owner).reduceSync(computationURI, reductor, combinator, currentAcc);
	}

	@Override
	public void clearMapReduceComputation(String computationURI) throws Exception {
		((NodeComponent) this.owner).clearMapReduceComputation(computationURI);
	} 
}