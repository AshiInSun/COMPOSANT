package defaultTeam.port;


import fr.sorbonne_u.components.ComponentI;
import fr.sorbonne_u.components.endpoints.EndPointI;
import fr.sorbonne_u.cps.dht_mapreduce.interfaces.mapreduce.MapReduceCI;
import fr.sorbonne_u.cps.dht_mapreduce.interfaces.mapreduce.MapReduceResultReceptionCI;
import fr.sorbonne_u.cps.dht_mapreduce.interfaces.mapreduce.CombinatorI;
import fr.sorbonne_u.cps.dht_mapreduce.interfaces.mapreduce.ProcessorI;
import fr.sorbonne_u.cps.dht_mapreduce.interfaces.mapreduce.ReductorI;
import fr.sorbonne_u.cps.dht_mapreduce.interfaces.mapreduce.SelectorI;

import java.io.Serializable;

public class DHTAsyncMapReduceOutboundPort extends DHTMapReduceOutboundPort implements MapReduceCI {
    private static final long serialVersionUID = 1L;
    
    public DHTAsyncMapReduceOutboundPort(String uri, ComponentI owner) throws Exception {
        super(uri, owner);
    }
    
    @Override
    public <R extends Serializable, I extends MapReduceResultReceptionCI> void map(String computationURI,
			SelectorI selector, ProcessorI<R> processor) throws Exception {
        ((MapReduceCI)this.getConnector()).map(computationURI, selector, processor);
    }
    
    @Override
    public <A extends Serializable, R, I extends MapReduceResultReceptionCI> void reduce(String computationURI,
			ReductorI<A, R> reductor, CombinatorI<A> combinator, A identityAcc, A currentAcc, EndPointI<I> callerNode)
			throws Exception {
        ((MapReduceCI)this.getConnector()).reduce(computationURI, reductor, combinator, identityAcc, currentAcc, callerNode);
    }
}
