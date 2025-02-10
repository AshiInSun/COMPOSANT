import java.io.Serializable;

import fr.sorbonne_u.cps.dht_mapreduce.interfaces.content.ContentDataI;
import fr.sorbonne_u.cps.dht_mapreduce.interfaces.content.ContentKeyI;
import fr.sorbonne_u.cps.dht_mapreduce.interfaces.frontend.DHTServicesI;
import fr.sorbonne_u.cps.dht_mapreduce.interfaces.mapreduce.CombinatorI;
import fr.sorbonne_u.cps.dht_mapreduce.interfaces.mapreduce.ProcessorI;
import fr.sorbonne_u.cps.dht_mapreduce.interfaces.mapreduce.ReductorI;
import fr.sorbonne_u.cps.dht_mapreduce.interfaces.mapreduce.SelectorI;
import fr.sorbonne_u.cps.mapreduce.utils.URIGenerator;

public class Facade implements DHTServicesI {
	
	private Node node;
	
	public Facade() {
		this.node = new Node(0, 99);
	}
	
	// initialisation d'une facade avec n noeuds (intervalle arbitraire de 100)
	public Facade(int n) {
		int c1 = 0;
		int c2 = 99;
		this.node = new Node(c1, c2);
		Node currNode = this.node;
		for(int i=0; i<(n-1); i++) {
			c1+=100;
			c2+=100;
			Node newNode = new Node(c1, c2);
			currNode.setSuiv(newNode);
			currNode = newNode;
		}
		currNode.setSuiv(this.node);
	}
	
	@Override
	public ContentDataI get(ContentKeyI key) throws Exception {
		ContentDataI value = node.getSync(null, key);
		node.clearComputation(null);
		return value;
	}

	@Override
	public ContentDataI put(ContentKeyI key, ContentDataI value) throws Exception {
		ContentDataI prev_value = node.getSync(null, key);
		node.putSync(null, key, value);
		node.clearComputation(null);
		return prev_value;
	}

	@Override
	public ContentDataI remove(ContentKeyI key) throws Exception {
		ContentDataI prev_value = node.getSync(null, key);
		node.removeSync(null, key);
		node.clearComputation(null);
		return prev_value;
	}
	
	@Override
	public <R extends Serializable, A extends Serializable> A mapReduce(SelectorI selector, ProcessorI<R> processor,
			ReductorI<A, R> reductor, CombinatorI<A> combinator, A initialAcc) throws Exception {
		if (selector == null || processor == null || reductor == null || combinator == null || initialAcc == null) 
			throw new IllegalArgumentException("Parametre(s) de mapReduce null "); 
		
		String computationURI = URIGenerator.generateURI("MAP_REDUCE");
		Node currNode = this.node;
		
		do {
	        currNode.mapSync(computationURI, selector, processor);
	        currNode = currNode.getSuiv(); 
	    } while (currNode != this.node); 

	    A res = initialAcc;
	    currNode = this.node;
	    do {
	        A partialResult = currNode.reduceSync(computationURI, reductor, combinator, initialAcc);
	        res = combinator.apply(res, partialResult);
	        currNode= currNode.getSuiv();
	    } while (currNode != this.node); 

	    currNode = this.node;
	    do {
	        currNode.clearMapReduceComputation(computationURI);
	        currNode = currNode.getSuiv();
	    } while (currNode != this.node);

	    return res;
	}	
}
