package defaultTeam;
import fr.sorbonne_u.components.AbstractComponent;
import fr.sorbonne_u.components.connectors.ConnectorI;
import fr.sorbonne_u.components.exceptions.ComponentShutdownException;
import fr.sorbonne_u.components.exceptions.ComponentStartException;
import fr.sorbonne_u.cps.dht_mapreduce.interfaces.content.ContentAccessSyncCI;
import fr.sorbonne_u.cps.dht_mapreduce.interfaces.content.ContentDataI;
import fr.sorbonne_u.cps.dht_mapreduce.interfaces.content.ContentKeyI;
import fr.sorbonne_u.cps.dht_mapreduce.interfaces.mapreduce.CombinatorI;
import fr.sorbonne_u.cps.dht_mapreduce.interfaces.mapreduce.MapReduceSyncCI;
import fr.sorbonne_u.cps.dht_mapreduce.interfaces.mapreduce.ProcessorI;
import fr.sorbonne_u.cps.dht_mapreduce.interfaces.mapreduce.ReductorI;
import fr.sorbonne_u.cps.dht_mapreduce.interfaces.mapreduce.SelectorI;
import java.io.Serializable;
import java.util.AbstractMap;
import java.util.HashMap;
import java.util.Map;
import java.util.stream.Collectors;

import defaultTeam.port.DHTContentAccessConnector;
import defaultTeam.port.DHTMapReduceConnector;
import defaultTeam.port.DHTServiceConnector;

public class NodeComponent extends AbstractComponent implements ContentAccessSyncCI, MapReduceSyncCI {

    private final int debut, fin;
    private final Map<ContentKeyI, ContentDataI> table;
    private Map<String, Map<ContentKeyI, Serializable>> mapResults;
    private boolean visite;
    private final BCMContentNodeCompositeEndPoint compositeEndpoint;
    
    public NodeComponent(int debut, int fin) throws Exception {
        super(1, 0);

        this.debut = debut;
        this.fin = fin;
        this.table = new HashMap<>();
        this.mapResults = new HashMap<>();
        this.visite = false;

        // Création des ports d’entrée et de sortie pour la communication BCM
        this.compositeEndpoint = new BCMContentNodeCompositeEndPoint();
        
        this.toggleTracing();
        this.toggleLogging();
        
        System.out.println("NodeComponent - Tracing activé.");
        System.out.println("NodeComponent initialisé avec les URIs suivants :");
        System.out.println("ContentAccess Endpoint URI : " + getContentAccessEndpointURI());
        System.out.println("MapReduce Endpoint URI : " + getMapReduceEndpointURI());
        System.out.println("Services Endpoint URI : " + getServicesEndpointURI());
        
        System.out.println(compositeEndpoint.getContentAccessEndpoint().getOutboundPortURI()) ;
        System.out.println(compositeEndpoint.getMapReduceEndpoint().getOutboundPortURI());
        System.out.println(compositeEndpoint.getServicesEndpoint().getOutboundPortURI()); 
               
        // Traces pour observer le cycle de vie
        this.traceMessage("NodeComponent initialisé avec les ports");
    }
    
    public String getContentAccessEndpointURI() {
        return this.compositeEndpoint.getContentAccessEndpoint().getInboundPortURI();
    }

    public String getMapReduceEndpointURI() {
        return this.compositeEndpoint.getMapReduceEndpoint().getInboundPortURI();
    }

    public String getServicesEndpointURI() {
        return this.compositeEndpoint.getServicesEndpoint().getInboundPortURI();
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
        super.finalise();
    }

    @Override
    public void shutdown() {
        try {
            try {
				compositeEndpoint.unpublishEndPoints();
			} catch (Exception e) {
				e.printStackTrace();
			}
            super.shutdown();
        } catch (ComponentShutdownException e) {
            e.printStackTrace();
        }
    }
    
    public void connectToNextNode(String nextNodeContentAccessURI, String nextNodeMapReduceURI, 
        String nextNodeServicesURI) throws Exception {
    	 System.out.println("Vérification si les ports sont publiés...");
	    System.out.println("ContentAccess Outbound Port URI : " + compositeEndpoint.getContentAccessEndpoint().getOutboundPortURI());
	    System.out.println("MapReduce Outbound Port URI : " + compositeEndpoint.getMapReduceEndpoint().getOutboundPortURI());
	    System.out.println("Services Outbound Port URI : " + compositeEndpoint.getServicesEndpoint().getOutboundPortURI());

        if (nextNodeContentAccessURI == null || nextNodeMapReduceURI == null || nextNodeServicesURI == null) {
            throw new Exception("Erreur : Un des URI de connexion est NULL !");
        }

		this.doPortConnection(
				compositeEndpoint.getContentAccessEndpoint().getOutboundPortURI(),
				nextNodeContentAccessURI,
				DHTContentAccessConnector.class.getCanonicalName());
	
		this.doPortConnection(
			compositeEndpoint.getMapReduceEndpoint().getOutboundPortURI(),
			nextNodeMapReduceURI,
			DHTMapReduceConnector.class.getCanonicalName());
	
		this.doPortConnection(
			compositeEndpoint.getServicesEndpoint().getOutboundPortURI(),
			nextNodeServicesURI,
			DHTServiceConnector.class.getCanonicalName());
	
	this.traceMessage("Nœud connecté au suivant.");
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
			
			if(((ConnectorI) compositeEndpoint.getContentAccessEndpoint()).connected()) {
				return ((ContentAccessSyncCI) compositeEndpoint.getContentAccessEndpoint()).getSync(computationURI, key);
			}else {
				throw new Exception("OutboundPort isn't connected.");
			}
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
			
			if(((ConnectorI) compositeEndpoint.getContentAccessEndpoint()).connected()) {
				return ((ContentAccessSyncCI) compositeEndpoint.getContentAccessEndpoint()).putSync(computationURI, key, value);
			}else {
				throw new Exception("OutboundPort isn't connected.");
			}
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
			
			return ((ContentAccessSyncCI) compositeEndpoint.getContentAccessEndpoint()).removeSync(computationURI, key);
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
			((ContentAccessSyncCI) compositeEndpoint.getContentAccessEndpoint()).clearComputation(computationURI);
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
		
		@SuppressWarnings("unchecked")
		A res = resultsInterm.values().stream()
			.map(value -> (R) value) 
			.reduce(currentAcc, reductor::apply, combinator::apply);

		return res;
	}
}
