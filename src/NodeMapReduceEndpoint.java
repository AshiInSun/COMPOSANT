import java.io.Serializable;
import fr.sorbonne_u.components.endpoints.EndPointI;
import fr.sorbonne_u.cps.dht_mapreduce.interfaces.mapreduce.CombinatorI;
import fr.sorbonne_u.cps.dht_mapreduce.interfaces.mapreduce.MapReduceSyncI;
import fr.sorbonne_u.cps.dht_mapreduce.interfaces.mapreduce.ProcessorI;
import fr.sorbonne_u.cps.dht_mapreduce.interfaces.mapreduce.ReductorI;
import fr.sorbonne_u.cps.dht_mapreduce.interfaces.mapreduce.SelectorI;

public class NodeMapReduceEndpoint implements EndPointI<MapReduceSyncI>{
	
	private final Node node;

    public NodeMapReduceEndpoint(Node node) {
        this.node = node;
    }

    public <R extends Serializable> void mapSync(String computationURI, SelectorI selector, ProcessorI<R> processor) throws Exception {
        node.mapSync(computationURI, selector, processor);
    }

    public <A extends Serializable, R> A reduceSync(String computationURI, ReductorI<A, R> reductor, CombinatorI<A> combinator, A currentAcc) throws Exception {
        return node.reduceSync(computationURI, reductor, combinator, currentAcc);
    }

    public void clearMapReduceComputation(String computationURI) throws Exception {
        node.clearMapReduceComputation(computationURI);
    }
}

