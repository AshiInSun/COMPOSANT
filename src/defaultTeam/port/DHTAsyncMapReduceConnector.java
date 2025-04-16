package defaultTeam.port;


import fr.sorbonne_u.cps.dht_mapreduce.interfaces.mapreduce.MapReduceCI;
import fr.sorbonne_u.cps.dht_mapreduce.interfaces.mapreduce.MapReduceResultReceptionCI;
import fr.sorbonne_u.cps.dht_mapreduce.interfaces.mapreduce.MapReduceSyncCI;
import fr.sorbonne_u.components.endpoints.EndPointI;
import fr.sorbonne_u.cps.dht_mapreduce.interfaces.mapreduce.CombinatorI;
import fr.sorbonne_u.cps.dht_mapreduce.interfaces.mapreduce.ProcessorI;
import fr.sorbonne_u.cps.dht_mapreduce.interfaces.mapreduce.ReductorI;
import fr.sorbonne_u.cps.dht_mapreduce.interfaces.mapreduce.SelectorI;
import java.io.Serializable;

import defaultTeam.port.sync.DHTMapReduceConnector;

public class DHTAsyncMapReduceConnector extends DHTMapReduceConnector implements MapReduceCI {

    @Override
    public <R extends Serializable> void map(String computationURI,
			SelectorI selector, ProcessorI<R> processor) throws Exception {
        ((MapReduceCI)this.offering).map(computationURI, selector, processor);
    }

    @Override
    public <A extends Serializable, R, I extends MapReduceResultReceptionCI> void reduce(String computationURI,
			ReductorI<A, R> reductor, CombinatorI<A> combinator, A identityAcc, A currentAcc, EndPointI<I> callerNode)
			throws Exception {
        ((MapReduceCI)this.offering).reduce(computationURI, reductor, combinator, identityAcc, currentAcc, callerNode);
    }
    @Override
	public void clearMapReduceComputation(String computationURI) throws Exception {
		((MapReduceCI) this.offering).clearMapReduceComputation(computationURI);
	}
}