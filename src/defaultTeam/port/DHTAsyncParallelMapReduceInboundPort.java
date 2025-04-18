package defaultTeam.port;

import fr.sorbonne_u.components.ComponentI;
import fr.sorbonne_u.cps.dht_mapreduce.interfaces.mapreduce.MapReduceCI;
import fr.sorbonne_u.cps.dht_mapreduce.interfaces.mapreduce.MapReduceResultReceptionCI;
import fr.sorbonne_u.cps.dht_mapreduce.interfaces.mapreduce.ParallelMapReduceCI;
import fr.sorbonne_u.components.endpoints.EndPointI;
import fr.sorbonne_u.cps.dht_mapreduce.interfaces.mapreduce.CombinatorI;
import fr.sorbonne_u.cps.dht_mapreduce.interfaces.mapreduce.ProcessorI;
import fr.sorbonne_u.cps.dht_mapreduce.interfaces.mapreduce.ReductorI;
import fr.sorbonne_u.cps.dht_mapreduce.interfaces.mapreduce.SelectorI;
import defaultTeam.NodeAsyncComponent;
import defaultTeam.old.NodeComponent;
import defaultTeam.port.sync.DHTMapReduceInboundPort;

import java.io.Serializable;

public class DHTAsyncParallelMapReduceInboundPort extends DHTMapReduceInboundPort implements ParallelMapReduceCI {
    private static final long serialVersionUID = 1L;
    public static final String MAP_REDUCE_HANDLER_URI = "mrah";

    public DHTAsyncParallelMapReduceInboundPort(String uri, ComponentI owner) throws Exception {
        super(uri, owner);
    }

	@Override
	public <R extends Serializable> void map(String computationURI, SelectorI selector, ProcessorI<R> processor)
			throws Exception {
		this.getOwner().runTask(MAP_REDUCE_HANDLER_URI, o -> {
	        try {
	            ((NodeAsyncComponent) o).map(computationURI, selector, processor);
	        } catch (Exception e) {
	            e.printStackTrace();
	        }
		});
	}

	@Override
	public <A extends Serializable, R, CI extends MapReduceResultReceptionCI> void reduce(String computationURI,
			ReductorI<A, R> reductor, CombinatorI<A> combinator, A identityAcc, A currentAcc, EndPointI<CI> callerNode)
			throws Exception {
		this.getOwner().runTask(MAP_REDUCE_HANDLER_URI, o -> {
	        try {
	            ((NodeAsyncComponent) o).reduce(computationURI, reductor, combinator, currentAcc, callerNode);
	        } catch (Exception e) {
	            e.printStackTrace();
	        }
		});
	}
	@Override
	public void clearMapReduceComputation(String computationURI) throws Exception {
		this.getOwner().runTask(MAP_REDUCE_HANDLER_URI, o -> {
	        try {
	        	((NodeAsyncComponent) o).clearMapReduceComputation(computationURI);
	        } catch (Exception e) {
	            e.printStackTrace();
	        }
		});
	}
	@Override
	public <R extends Serializable> void parallelMap(String computationURI, SelectorI selector, ProcessorI<R> processor,
			ParallelismPolicyI parallelismPolicy) throws Exception {
		this.getOwner().runTask(MAP_REDUCE_HANDLER_URI, o -> {
	        try {
	            ((NodeAsyncComponent) o).parallelMap(computationURI, selector, processor, parallelismPolicy);
	        } catch (Exception e) {
	            e.printStackTrace();
	        }
		});
	}



	@Override
	public <A extends Serializable, R, I extends MapReduceResultReceptionCI> void parallelReduce(String computationURI,
			ReductorI<A, R> reductor, CombinatorI<A> combinator, A identityAcc, A currentAcc,
			ParallelismPolicyI parallelismPolicy, EndPointI<I> caller) throws Exception {
		this.getOwner().runTask(MAP_REDUCE_HANDLER_URI, o -> {
	        try {
	            ((NodeAsyncComponent) o).parallelReduce(computationURI, reductor, combinator, identityAcc, currentAcc, parallelismPolicy, caller);
	        } catch (Exception e) {
	            e.printStackTrace();
	        }
		});
	} 
}
