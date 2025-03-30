package defaultTeam.port;

import fr.sorbonne_u.components.ComponentI;
import fr.sorbonne_u.cps.dht_mapreduce.interfaces.mapreduce.MapReduceCI;
import fr.sorbonne_u.cps.dht_mapreduce.interfaces.mapreduce.MapReduceResultReceptionCI;
import fr.sorbonne_u.components.endpoints.EndPointI;
import fr.sorbonne_u.cps.dht_mapreduce.interfaces.mapreduce.CombinatorI;
import fr.sorbonne_u.cps.dht_mapreduce.interfaces.mapreduce.ProcessorI;
import fr.sorbonne_u.cps.dht_mapreduce.interfaces.mapreduce.ReductorI;
import fr.sorbonne_u.cps.dht_mapreduce.interfaces.mapreduce.SelectorI;
import defaultTeam.AsyncNodeComponent;

import java.io.Serializable;

public class DHTAsyncMapReduceInboundPort extends DHTMapReduceInboundPort implements MapReduceCI {
    private static final long serialVersionUID = 1L;

    public DHTAsyncMapReduceInboundPort(String uri, ComponentI owner) throws Exception {
        super(uri, owner);
    }

	@Override
	public <R extends Serializable, I extends MapReduceResultReceptionCI> void map(String computationURI,
			SelectorI selector, ProcessorI<R> processor) throws Exception {
        try {
            ((AsyncNodeComponent) owner).mapSync(computationURI, selector, processor);
        } catch (Exception e) {
            e.printStackTrace();
        }
	}

	@Override
	public <A extends Serializable, R, I extends MapReduceResultReceptionCI> void reduce(String computationURI,
			ReductorI<A, R> reductor, CombinatorI<A> combinator, A identityAcc, A currentAcc, EndPointI<I> callerNode)
			throws Exception {
        try {
            A result = ((AsyncNodeComponent) owner).reduceSync(computationURI, reductor, combinator, currentAcc);
            String emitterId = ((AsyncNodeComponent) owner).getURI();
            callerNode.getClientSideReference().acceptResult(computationURI, emitterId, result);
        } catch (Exception e) {
            e.printStackTrace();
        }
	}

}
