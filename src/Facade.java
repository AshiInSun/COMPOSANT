import java.io.Serializable;

import fr.sorbonne_u.cps.dht_mapreduce.interfaces.content.ContentDataI;
import fr.sorbonne_u.cps.dht_mapreduce.interfaces.content.ContentKeyI;
import fr.sorbonne_u.cps.dht_mapreduce.interfaces.frontend.DHTServicesI;
import fr.sorbonne_u.cps.dht_mapreduce.interfaces.mapreduce.CombinatorI;
import fr.sorbonne_u.cps.dht_mapreduce.interfaces.mapreduce.ProcessorI;
import fr.sorbonne_u.cps.dht_mapreduce.interfaces.mapreduce.ReductorI;
import fr.sorbonne_u.cps.dht_mapreduce.interfaces.mapreduce.SelectorI;
import fr.sorbonne_u.cps.mapreduce.endpoints.POJOContentNodeCompositeEndPoint;
import fr.sorbonne_u.cps.mapreduce.utils.URIGenerator;

public class Facade implements DHTServicesI {
	
	POJOContentNodeCompositeEndPoint edp_server;
	
	public Facade(Node node) {	
		this.edp_server = node.edp_client;
		edp_server.initialiseClientSide(this.edp_server);
	}
	
	@Override
	public ContentDataI get(ContentKeyI key) throws Exception {
		ContentDataI value = edp_server.getContentAccessEndpoint().getClientSideReference().getSync(null, key);
		edp_server.getContentAccessEndpoint().getClientSideReference().clearComputation(null);
		return value;
	}

	@Override
	public ContentDataI put(ContentKeyI key, ContentDataI value) throws Exception {
		ContentDataI prev_value = edp_server.getContentAccessEndpoint().getClientSideReference().putSync(null, key, value);
		edp_server.getContentAccessEndpoint().getClientSideReference().clearComputation(null);
		return prev_value;
	}

	@Override
	public ContentDataI remove(ContentKeyI key) throws Exception {
		ContentDataI prev_value = edp_server.getContentAccessEndpoint().getClientSideReference().removeSync(null, key);
		edp_server.getContentAccessEndpoint().getClientSideReference().clearComputation(null);
		return prev_value;
	}
	
	@Override
	public <R extends Serializable, A extends Serializable> A mapReduce(SelectorI selector, ProcessorI<R> processor,
			ReductorI<A, R> reductor, CombinatorI<A> combinator, A initialAcc) throws Exception {
		if (selector == null || processor == null || reductor == null || combinator == null || initialAcc == null) 
			throw new IllegalArgumentException("Parametre(s) de mapReduce null "); 
		
		String computationURI = URIGenerator.generateURI("MAP_REDUCE");
		POJOContentNodeCompositeEndPoint currentEdp = this.edp_server;
		
		do {
			currentEdp.getMapReduceEndpoint().getClientSideReference().mapSync(computationURI, selector, processor);
			currentEdp = ((Node) currentEdp.getContentAccessEndpoint().getClientSideReference()).getNext();
	    } while (currentEdp != this.edp_server); 

	    A res = initialAcc;
	    currentEdp = this.edp_server;
	    
	    do {
	        A partialResult = currentEdp.getMapReduceEndpoint().getClientSideReference()
	                					.reduceSync(computationURI, reductor, combinator, initialAcc);
	        res = combinator.apply(res, partialResult);
	        currentEdp = ((Node) currentEdp.getContentAccessEndpoint().getClientSideReference()).getNext();
	    } while (currentEdp != this.edp_server); 

	    currentEdp = this.edp_server;
	    do {
	    	currentEdp.getMapReduceEndpoint().getClientSideReference().clearMapReduceComputation(computationURI);
	        currentEdp = ((Node) currentEdp.getContentAccessEndpoint().getClientSideReference()).getNext();
	    } while (currentEdp != this.edp_server);

	    return res;
	}	
}
