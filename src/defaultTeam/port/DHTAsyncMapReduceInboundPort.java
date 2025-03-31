package defaultTeam.port;

import fr.sorbonne_u.components.ComponentI;
import fr.sorbonne_u.cps.dht_mapreduce.interfaces.mapreduce.MapReduceCI;
import fr.sorbonne_u.cps.dht_mapreduce.interfaces.mapreduce.MapReduceResultReceptionCI;
import fr.sorbonne_u.components.endpoints.EndPointI;
import fr.sorbonne_u.cps.dht_mapreduce.interfaces.mapreduce.CombinatorI;
import fr.sorbonne_u.cps.dht_mapreduce.interfaces.mapreduce.ProcessorI;
import fr.sorbonne_u.cps.dht_mapreduce.interfaces.mapreduce.ReductorI;
import fr.sorbonne_u.cps.dht_mapreduce.interfaces.mapreduce.SelectorI;
import defaultTeam.NodeAsyncComponent;
import defaultTeam.old.NodeComponent;
import defaultTeam.port.sync.DHTMapReduceInboundPort;

import java.io.Serializable;

public class DHTAsyncMapReduceInboundPort extends DHTMapReduceInboundPort implements MapReduceCI {
    private static final long serialVersionUID = 1L;

    public DHTAsyncMapReduceInboundPort(String uri, ComponentI owner) throws Exception {
        super(uri, owner);
    }

	@Override
	public <R extends Serializable, I extends MapReduceResultReceptionCI> void map(String computationURI,
			SelectorI selector, ProcessorI<R> processor) throws Exception {
		this.getOwner().runTask(o -> {
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
		this.getOwner().runTask(o -> {
	        try {
	            ((NodeAsyncComponent) o).reduce(computationURI, reductor, combinator, currentAcc, callerNode);
	        } catch (Exception e) {
	            e.printStackTrace();
	        }
		});
	}
	@Override
	public void clearMapReduceComputation(String computationURI) throws Exception {
		this.getOwner().runTask(o -> {
	        try {
	        	((NodeAsyncComponent) o).clearMapReduceComputation(computationURI);
	        } catch (Exception e) {
	            e.printStackTrace();
	        }
		});
	} 
}
