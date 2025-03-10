package defaultTeam;
import fr.sorbonne_u.components.AbstractComponent;
import fr.sorbonne_u.components.annotations.OfferedInterfaces;
import fr.sorbonne_u.components.annotations.RequiredInterfaces;
import fr.sorbonne_u.components.exceptions.ComponentShutdownException;
import fr.sorbonne_u.components.exceptions.ComponentStartException;
import fr.sorbonne_u.components.exceptions.ConnectionException;

import fr.sorbonne_u.cps.dht_mapreduce.interfaces.content.ContentAccessSyncCI;
import fr.sorbonne_u.cps.dht_mapreduce.interfaces.content.ContentDataI;
import fr.sorbonne_u.cps.dht_mapreduce.interfaces.content.ContentKeyI;
import fr.sorbonne_u.cps.dht_mapreduce.interfaces.frontend.DHTServicesCI;
import fr.sorbonne_u.cps.dht_mapreduce.interfaces.mapreduce.MapReduceSyncCI;
import fr.sorbonne_u.cps.dht_mapreduce.interfaces.mapreduce.CombinatorI;
import fr.sorbonne_u.cps.dht_mapreduce.interfaces.mapreduce.ProcessorI;
import fr.sorbonne_u.cps.dht_mapreduce.interfaces.mapreduce.ReductorI;
import fr.sorbonne_u.cps.dht_mapreduce.interfaces.mapreduce.SelectorI;
import fr.sorbonne_u.cps.mapreduce.utils.IntInterval;

import java.io.Serializable;
import java.util.AbstractMap;
import java.util.HashMap;
import java.util.Map;
import java.util.stream.Collectors;
import java.util.stream.Stream;

@OfferedInterfaces(offered = {ContentAccessSyncCI.class, MapReduceSyncCI.class, DHTServicesCI.class})
@RequiredInterfaces(required = {ContentAccessSyncCI.class, MapReduceSyncCI.class})
public class NodeComponent extends AbstractComponent {
	
	private IntInterval interval;
    private final int next_deb;	// TEMPORAIRE
    private final Map<ContentKeyI, ContentDataI> table;
    HashMap<String,Stream<ContentDataI>> streamMap;
    private Map<String, Map<ContentKeyI, Serializable>> mapResults;
    private Map<String, Boolean> visited;
    BCMContentNodeCompositeEndPoint client_edp; //me
    BCMContentNodeCompositeEndPoint server_edp; //the next
    //
    BCMContentNodeCompositeEndPoint dht_edp; //only for the first node : connexion to facade
    
    protected NodeComponent(String uri, int debut, int fin, int next_deb,
		BCMContentNodeCompositeEndPoint dht_edp,
		BCMContentNodeCompositeEndPoint client_edp,
		BCMContentNodeCompositeEndPoint server_edp) throws Exception {
    	
        super(1, 0);

        this.interval = new IntInterval(debut, fin);
        this.next_deb = next_deb;
        this.table = new HashMap<>();
        this.mapResults = new HashMap<>();
        this.streamMap = new HashMap<String, Stream<ContentDataI>>();
        this.visited = new HashMap<>();
        this.client_edp = client_edp;
        this.server_edp = server_edp;
        if(debut==0) {
        	System.out.println("First Node");
        	this.dht_edp = dht_edp;
        }else {
        	this.dht_edp = null;
        }
        
        this.toggleLogging();
        
        client_edp.initialiseServerSide(this);
        if(debut==0) {
        	dht_edp.initialiseServerSide(this);
        }
        System.out.println("NodeComponent initialisé.");
    }
    @Override
    public void start() throws ComponentStartException {
    	try {
			server_edp.initialiseClientSide(this);
		} catch (ConnectionException e) {
			e.printStackTrace();
		}
        super.start();
    }

    @Override
    public void finalise() throws Exception {
    	server_edp.cleanUpClientSide();
    	super.finalise();
    }

    @Override
    public void shutdown() throws ComponentShutdownException {
        if(interval.first() == 0) {
        	dht_edp.cleanUpServerSide();
        }
        client_edp.cleanUpServerSide();
        super.shutdown();
    }
    
    public boolean existStream(String computationURI) {
		return streamMap.containsKey(computationURI);
	}
    
	public ContentDataI getSync(String computationURI, ContentKeyI key) throws Exception {	
		int h = key.hashCode();
		
		if ( interval.in(h) ) {
			return table.get(key);
		}
		else {
			if (visited.containsKey(computationURI))
				return null;	
			
			visited.put(computationURI, true);
			return (server_edp.getContentAccessEndpoint()).getClientSideReference().getSync(computationURI, key);
		}
	}

	public ContentDataI putSync(String computationURI, ContentKeyI key, ContentDataI value) throws Exception {
		int h = key.hashCode();
		
		if ( interval.in(h) ) {
			return table.put(key, value);
		}
		else {
			if (visited.containsKey(computationURI))
				return null;
			
			visited.put(computationURI, true);
			return (server_edp.getContentAccessEndpoint().getClientSideReference()).putSync(computationURI, key, value);
		}
	}

	public ContentDataI removeSync(String computationURI, ContentKeyI key) throws Exception {
		int h = key.hashCode();
		
		if ( interval.in(h) ) {
			return table.remove(key);
		}
		else {
			if (visited.containsKey(computationURI))
				return null;
			
			visited.put(computationURI, true);
			return (server_edp.getContentAccessEndpoint().getClientSideReference()).removeSync(computationURI, key);
		}
	}
	
	public void clearMapReduceComputation(String computationURI) throws Exception {
		if (computationURI == null || computationURI.isEmpty() )
			System.out.print("Parametre(s) de reduceSync null");
		mapResults.remove(computationURI);
	}

	public void clearComputation(String computationURI) throws Exception {
		if (visited.containsKey(computationURI)) {
			visited.remove(computationURI);
			(server_edp.getContentAccessEndpoint().getClientSideReference()).clearComputation(computationURI);
		}
	}
	
	public <R extends Serializable> void mapSync(String computationURI, SelectorI selector, ProcessorI<R> processor) throws Exception {
		if (computationURI == null || computationURI.isEmpty() || selector == null || processor == null) 
	        throw new IllegalArgumentException("Parametre(s) de mapSync null ");    		
		
        Map<ContentKeyI, Serializable> results = table.entrySet().stream()
            .filter(entry -> selector.test(entry.getValue()))
            .map(entry -> new AbstractMap.SimpleEntry<>(entry.getKey(), processor.apply(entry.getValue())))
            .collect(Collectors.toMap(Map.Entry::getKey, Map.Entry::getValue));
               
        mapResults.put(computationURI, results);
        
        if (next_deb != 0) {
        server_edp.getMapReduceEndpoint().getClientSideReference().mapSync(computationURI, selector, processor);
        }
	}

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
		
		if (next_deb != 0) {
			res = server_edp.getMapReduceEndpoint().getClientSideReference().reduceSync(computationURI, reductor, combinator, res);
		}

		return res;
	}
}
