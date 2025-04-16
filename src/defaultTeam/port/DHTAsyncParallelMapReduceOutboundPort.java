package defaultTeam.port;


import fr.sorbonne_u.components.ComponentI;
import fr.sorbonne_u.components.endpoints.EndPointI;
import fr.sorbonne_u.cps.dht_mapreduce.interfaces.mapreduce.MapReduceCI;
import fr.sorbonne_u.cps.dht_mapreduce.interfaces.mapreduce.MapReduceResultReceptionCI;
import fr.sorbonne_u.cps.dht_mapreduce.interfaces.mapreduce.MapReduceSyncCI;
import fr.sorbonne_u.cps.dht_mapreduce.interfaces.mapreduce.ParallelMapReduceCI;
import fr.sorbonne_u.cps.dht_mapreduce.interfaces.mapreduce.CombinatorI;
import fr.sorbonne_u.cps.dht_mapreduce.interfaces.mapreduce.ProcessorI;
import fr.sorbonne_u.cps.dht_mapreduce.interfaces.mapreduce.ReductorI;
import fr.sorbonne_u.cps.dht_mapreduce.interfaces.mapreduce.SelectorI;

import java.io.Serializable;

import defaultTeam.port.sync.DHTMapReduceOutboundPort;

public class DHTAsyncParallelMapReduceOutboundPort extends DHTMapReduceOutboundPort implements ParallelMapReduceCI {
    private static final long serialVersionUID = 1L;
    
    public DHTAsyncParallelMapReduceOutboundPort(String uri, ComponentI owner) throws Exception {
        super(uri, owner);
    }
    
    @Override
	public <R extends Serializable> void map(String computationURI, SelectorI selector, ProcessorI<R> processor)
			throws Exception {
    	((ParallelMapReduceCI)this.getConnector()).map(computationURI, selector, processor);
	}
    @Override
    public <A extends Serializable, R, I extends MapReduceResultReceptionCI> void reduce(String computationURI,
			ReductorI<A, R> reductor, CombinatorI<A> combinator, A identityAcc, A currentAcc, EndPointI<I> callerNode)
			throws Exception {
        ((ParallelMapReduceCI)this.getConnector()).reduce(computationURI, reductor, combinator, identityAcc, currentAcc, callerNode);
    }
    @Override
    public void clearMapReduceComputation(String computationURI) throws Exception {
		((ParallelMapReduceCI) this.getConnector()).clearMapReduceComputation(computationURI);
	}

	@Override
	public <R extends Serializable> void parallelMap(String computationURI, SelectorI selector, ProcessorI<R> processor,
			ParallelismPolicyI parallelismPolicy) throws Exception {
		// TODO Auto-generated method stub
		
	}

	@Override
	public <A extends Serializable, R, I extends MapReduceResultReceptionCI> void parallelReduce(String computationURI,
			ReductorI<A, R> reductor, CombinatorI<A> combinator, A identityAcc, A currentAcc,
			ParallelismPolicyI parallelismPolicy, EndPointI<I> caller) throws Exception {
		// TODO Auto-generated method stub
		
	}
}
