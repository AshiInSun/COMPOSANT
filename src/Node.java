import java.io.Serializable;

import fr.sorbonne_u.cps.dht_mapreduce.interfaces.content.ContentAccessSyncI;
import fr.sorbonne_u.cps.dht_mapreduce.interfaces.content.ContentDataI;
import fr.sorbonne_u.cps.dht_mapreduce.interfaces.content.ContentKeyI;
import fr.sorbonne_u.cps.dht_mapreduce.interfaces.mapreduce.CombinatorI;
import fr.sorbonne_u.cps.dht_mapreduce.interfaces.mapreduce.MapReduceSyncI;
import fr.sorbonne_u.cps.dht_mapreduce.interfaces.mapreduce.ProcessorI;
import fr.sorbonne_u.cps.dht_mapreduce.interfaces.mapreduce.ReductorI;
import fr.sorbonne_u.cps.dht_mapreduce.interfaces.mapreduce.SelectorI;
import java.util.HashMap;
import java.util.AbstractMap;
import java.util.Map;
import java.util.stream.Collectors;

public class Node implements ContentAccessSyncI, MapReduceSyncI {
	
	private int debut, fin;
	private Map<ContentKeyI, ContentDataI > table;
	private Node suiv;
	private Map<String, Map<ContentKeyI, Serializable>> mapResults;
	
	public Node(int debut, int fin, Node node) {
		this.debut = debut;
		this.fin   = fin;
		this.table = new HashMap<>();
		this.suiv = node;
		this.mapResults = new HashMap<>();
	}
	
	public Node(int debut, int fin) {
		this.debut = debut;
		this.fin   = fin;
		this.table = new HashMap<>();
		this.suiv = this;
		this.mapResults = new HashMap<>();
	}
	
	@Override
	public ContentDataI getSync(String computationURI, ContentKeyI key) throws Exception {	
		int h = key.hashCode();
		if ( debut <= h && h <= fin ) {
			return table.get(key);
		}
		else {
			if (suiv.getDebut() == 0)
				return null;		
			return suiv.getSync(null, key);
		}
	}

	@Override
	public ContentDataI putSync(String computationURI, ContentKeyI key, ContentDataI value) throws Exception {
		int h = key.hashCode();
		if ( debut <= h && h <= fin ) {
			ContentDataI prev_value = table.get(key);
			table.put(key, value);
			return prev_value;
		}
		else {
			if (suiv.getDebut() == 0)
				return null;
			
			// Temporaire, juste pour les tests
			System.out.println("On est passé au noeud suivant avec la clef " + ((ContentKey) key).getKey() + " qui a comme hash: " + h +"\n");
			
			return suiv.putSync(null, key, value);
		}
	}

	@Override
	public ContentDataI removeSync(String computationURI, ContentKeyI key) throws Exception {
		int h = key.hashCode();
		if ( debut <= h && h <= fin ) {
			ContentDataI prev_value = table.get(key);
			table.remove(key);
			return prev_value;
		}
		else {
			if (suiv.getDebut() == 0)
				return null;		
			return suiv.removeSync(null, key);
		}
	}
	
	@Override
	public void clearMapReduceComputation(String computationURI) throws Exception {
		if (computationURI == null || computationURI.isEmpty() )
			System.out.print("Parametre(s) de reduceSync null");
		mapResults.remove(computationURI);
	}

	@Override
	public void clearComputation(String computationURI) throws Exception {
		// A FAIRE SI : 
		// On a besoin de clean des artefacts derrière nous
	}
	
	@Override
	public <R extends Serializable> void mapSync(String computationURI, SelectorI selector, ProcessorI<R> processor) throws Exception {
		if (computationURI == null || computationURI.isEmpty() || selector == null || processor == null) 
	        throw new IllegalArgumentException("Parametre(s) de mapSync null ");    
	        
        Map<ContentKeyI, Serializable> results = table.entrySet().stream()
            .filter(entry -> selector.test(entry.getValue()))
            .map(entry -> new AbstractMap.SimpleEntry<>(entry.getKey(), processor.apply(entry.getValue())))
            .collect(Collectors.toMap(Map.Entry::getKey, Map.Entry::getValue));
               
        mapResults.put(computationURI, results);
	}

	@Override
	public <A extends Serializable, R> A reduceSync(String computationURI, ReductorI<A, R> reductor, CombinatorI<A> combinator, A currentAcc)
			throws Exception {
		if (computationURI == null || computationURI.isEmpty() || reductor == null || combinator == null) {
	        throw new IllegalArgumentException("Parametre(s) de reduceSync null ");    
		}
		
		Map<ContentKeyI, Serializable> resultsInterm = mapResults.get(computationURI);
		
		if (resultsInterm == null) {
	        throw new IllegalStateException("Aucun résultat trouvé pour ce computationURI: " + computationURI);
	    }
		
		A res = resultsInterm.values().stream()
			.map(value -> (R) value) 
			.reduce(currentAcc, reductor::apply, combinator::apply);

		return res;
	}
	
	public int getDebut() {
		return this.debut;
	}
	
	public Node getSuiv() {
		return this.suiv;
	}
	
	public void setSuiv(Node newSuiv) {
		this.suiv = newSuiv;
		return;
	}
	

}
