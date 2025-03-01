import fr.sorbonne_u.components.AbstractComponent;
import fr.sorbonne_u.components.exceptions.ComponentStartException;
import fr.sorbonne_u.components.exceptions.ConnectionException;
import fr.sorbonne_u.cps.dht_mapreduce.interfaces.content.ContentAccessSyncCI;
import fr.sorbonne_u.cps.dht_mapreduce.interfaces.content.ContentDataI;
import fr.sorbonne_u.cps.dht_mapreduce.interfaces.content.ContentKeyI;
import fr.sorbonne_u.cps.dht_mapreduce.interfaces.mapreduce.CombinatorI;
import fr.sorbonne_u.cps.dht_mapreduce.interfaces.mapreduce.MapReduceSyncCI;
import fr.sorbonne_u.cps.dht_mapreduce.interfaces.mapreduce.ProcessorI;
import fr.sorbonne_u.cps.dht_mapreduce.interfaces.mapreduce.ReductorI;
import fr.sorbonne_u.cps.dht_mapreduce.interfaces.mapreduce.SelectorI;
import fr.sorbonne_u.cps.dht_mapreduce.interfaces.frontend.DHTServicesCI;

import java.io.Serializable;
import java.util.AbstractMap;
import java.util.HashMap;
import java.util.Map;
import java.util.stream.Collectors;

// TODO: corriger les erreurs 

public class NodeComponent extends AbstractComponent implements ContentAccessSyncCI, MapReduceSyncCI {

    private final int debut, fin;
    private final Map<ContentKeyI, ContentDataI> table;
    private Map<String, Map<ContentKeyI, Serializable>> mapResults;
    private boolean visite;
    private final DHTInboundPort inboundPort;
    private final DHTOutboundPort outboundPort;

    public NodeComponent(int debut, int fin, String inboundURI, String outboundURI) throws Exception {
        super(1, 0);

        this.debut = debut;
        this.fin = fin;
        this.table = new HashMap<>();
        this.mapResults = new HashMap<>();
        this.visite = false;

        // Création des ports d’entrée et de sortie pour la communication BCM
        this.inboundPort = new DHTInboundPort(inboundURI, this);
        this.outboundPort = new DHTOutboundPort(outboundURI, this);

        // Publication des ports
        this.inboundPort.publishPort();
        this.outboundPort.publishPort();

        // Traces pour observer le cycle de vie
        this.traceMessage("NodeComponent initialisé avec les ports : " + inboundURI + " / " + outboundURI);
    }

    @Override
    public void start() throws ComponentStartException {
        super.start();
        this.traceMessage("NodeComponent démarré.");
    }

    @Override
    public void execute() throws Exception {
        this.traceMessage("NodeComponent exécute ses opérations...");
        // Faudra rajouter du code ici
    }

    @Override
    public void finalise() throws Exception {
        this.traceMessage("NodeComponent se termine...");
        this.doPortDisconnection(this.outboundPort.getPortURI());
        super.finalise();
    }

    @Override
    public void shutdown() throws Exception {
        this.inboundPort.unpublishPort();
        this.outboundPort.unpublishPort();
        super.shutdown();
    }

    public String getInboundPortURI() throws Exception {
        return this.inboundPort.getPortURI();
    }

    public String getOutboundPortURI() throws Exception {
        return this.outboundPort.getPortURI();
    }
    
    @Override
	public ContentDataI getSync(String computationURI, ContentKeyI key) throws Exception {	
		int h = key.hashCode();
		
		if ( debut <= h && h <= fin ) {
			return table.get(key);
		}
		else {
			if (this.visite)
				return null;	
			
			this.visite = true;
			return this.outboundPort.get(key);
		}
	}

	@Override
	public ContentDataI putSync(String computationURI, ContentKeyI key, ContentDataI value) throws Exception {
		int h = key.hashCode();
		
		if ( debut <= h && h <= fin ) {
			return table.put(key, value);
		}
		else {
			if (this.visite)
				return null;
			
			this.visite = true;
			return this.outboundPort.put(key, value);
		}
	}

	@Override
	public ContentDataI removeSync(String computationURI, ContentKeyI key) throws Exception {
		int h = key.hashCode();
		
		if ( debut <= h && h <= fin ) {
			return table.remove(key);
		}
		else {
			if (this.visite)
				return null;
			
			return this.outboundPort.remove(key);
		}
	}
	
	@Override
	public void clearMapReduceComputation(String computationURI) throws Exception {
		if (computationURI == null || computationURI.isEmpty() )
			System.out.print("Parametre(s) de reduceSync null");
		mapResults.remove(computationURI);
	}

	@Override
	// NOTE: Faudra modifier la facon de faire quand on passera en multi-threading ( on utilisera le computationURI avec une hashmap IG )
	public void clearComputation(String computationURI) throws Exception {
		if (this.visite) {
			this.visite = false;
			
			// Probleme: outboundPort n'a pas de methode clearComputation, solution temporaire -> rajouter cette methode
			//           dans HDTOutboundPort et dans DHTConnector			
			// 			 Ou sinon il faut trouver un moyen de clearComputation sans passer au noeud suivant, peut etre que
			//           la solution est offerte grace a certaines classes/methode de BCM
			this.outboundPort.clearComputation(computationURI);
		}
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
}
