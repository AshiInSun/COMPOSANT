package defaultTeam;
import fr.sorbonne_u.components.AbstractComponent;

import fr.sorbonne_u.components.annotations.OfferedInterfaces;
import fr.sorbonne_u.components.annotations.RequiredInterfaces;
import fr.sorbonne_u.components.endpoints.EndPoint;
import fr.sorbonne_u.components.endpoints.EndPointI;
import fr.sorbonne_u.components.exceptions.ComponentShutdownException;
import fr.sorbonne_u.components.exceptions.ComponentStartException;
import fr.sorbonne_u.components.exceptions.ConnectionException;

import fr.sorbonne_u.cps.dht_mapreduce.interfaces.content.ContentAccessSyncCI;
import fr.sorbonne_u.cps.dht_mapreduce.interfaces.content.ContentDataI;
import fr.sorbonne_u.cps.dht_mapreduce.interfaces.content.ContentKeyI;
import fr.sorbonne_u.cps.dht_mapreduce.interfaces.content.ResultReceptionCI;
import fr.sorbonne_u.cps.dht_mapreduce.interfaces.frontend.DHTServicesCI;
import fr.sorbonne_u.cps.dht_mapreduce.interfaces.mapreduce.MapReduceSyncCI;
import fr.sorbonne_u.cps.dht_mapreduce.interfaces.mapreduce.CombinatorI;
import fr.sorbonne_u.cps.dht_mapreduce.interfaces.mapreduce.ProcessorI;
import fr.sorbonne_u.cps.dht_mapreduce.interfaces.mapreduce.ReductorI;
import fr.sorbonne_u.cps.dht_mapreduce.interfaces.mapreduce.SelectorI;
import fr.sorbonne_u.cps.mapreduce.utils.IntInterval;
import fr.sorbonne_u.cps.dht_mapreduce.interfaces.mapreduce.MapReduceCI;
import fr.sorbonne_u.cps.dht_mapreduce.interfaces.mapreduce.MapReduceResultReceptionCI;
import fr.sorbonne_u.cps.dht_mapreduce.interfaces.content.ContentAccessCI;


import java.io.Serializable;
import java.util.HashMap;
import java.util.Map;
import java.util.stream.Stream;

@OfferedInterfaces(offered = {ContentAccessSyncCI.class, MapReduceSyncCI.class, 
        ContentAccessCI.class, MapReduceCI.class, 
        DHTServicesCI.class})
@RequiredInterfaces(required = {ContentAccessSyncCI.class, MapReduceSyncCI.class, 
        ContentAccessCI.class, MapReduceCI.class})
public class AsyncNodeComponent extends AbstractComponent {
	
	private IntInterval interval;
	private String uri;
	
	
    private final Map<ContentKeyI, ContentDataI> table;
    HashMap<String,Stream<ContentDataI>> streamMap;
    
    private Map<String, Boolean> visited;
    private Map<String, Boolean> visitedMap;	// On peut optimiser ces deux hashmap visited pour map reduce
    private Map<String, Boolean> visitedReduce;
    
    BCMAsyncContentNodeCompositeEndPoint client_edp; //me
    BCMAsyncContentNodeCompositeEndPoint server_edp; //the next
    BCMAsyncContentNodeCompositeEndPoint dht_edp; //only for the first node : connexion to facade
    
    protected AsyncNodeComponent(String uri, int debut, int fin,
		BCMAsyncContentNodeCompositeEndPoint dht_edp,
		BCMAsyncContentNodeCompositeEndPoint client_edp,
		BCMAsyncContentNodeCompositeEndPoint server_edp) throws Exception {
    	
        super(1, 0);

        this.interval = new IntInterval(debut, fin);
        this.uri = uri;
        this.table = new HashMap<>();
        this.streamMap = new HashMap<String, Stream<ContentDataI>>();
        this.visited = new HashMap<>();
        this.visitedMap = new HashMap<>();
        this.visitedReduce = new HashMap<>();
        this.client_edp = client_edp;
        this.server_edp = server_edp;
        if(debut==0) {
        	this.dht_edp = dht_edp;
        }else {
        	this.dht_edp = null;
        }
        
        this.toggleLogging();
        
        client_edp.initialiseServerSide(this);
        if(debut==0) {
        	dht_edp.initialiseServerSide(this);
        }
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
    
    public String getURI() {
    	return this.uri;
    }
    
    //Méthodes Asynchrones
    public <CI extends ResultReceptionCI> void get(
    		String computationURI, ContentKeyI key, EndPointI<CI> caller) throws Exception {
    		int h = key.hashCode();
		
			if ( interval.in(h) ) {
				ContentDataI result = table.get(key);
				caller.initialiseClientSide(this);
		        caller.getClientSideReference().acceptResult(computationURI, result);
		        caller.cleanUpClientSide();

			}
			else {
				if (visited.containsKey(computationURI)) {
					caller.initialiseClientSide(this);
					caller.getClientSideReference().acceptResult(computationURI, null);
					caller.cleanUpClientSide();
					return;
				}
				
				visited.put(computationURI, true);
				(server_edp.getContentAccessEndpoint()).getClientSideReference().get(computationURI, key, caller.copyWithSharable());
			}
    }
    
    public <CI extends ResultReceptionCI> void put(
    		String computationURI, ContentKeyI key, ContentDataI value, EndPointI<CI> caller) throws Exception {
    	
		int h = key.hashCode();
		
		if ( interval.in(h) ) {
			ContentDataI result =  table.put(key, value);
			caller.initialiseClientSide(this);
	        caller.getClientSideReference().acceptResult(computationURI, result);
	        caller.cleanUpClientSide();
		}
		else {
			if (visited.containsKey(computationURI)){
				caller.initialiseClientSide(this);
				caller.getClientSideReference().acceptResult(computationURI, null);
				caller.cleanUpClientSide();
				return;
			}
			
			visited.put(computationURI, true);
			(server_edp.getContentAccessEndpoint().getClientSideReference()).put(computationURI, key, value, caller.copyWithSharable());
		}
	}
    public <CI extends ResultReceptionCI> void remove(String computationURI, ContentKeyI key, EndPointI<CI> caller) throws Exception {
		int h = key.hashCode();
		
		if ( interval.in(h) ) {
			ContentDataI result =  table.remove(key);
	        caller.getClientSideReference().acceptResult(computationURI, result);
		}
		else {
			if (visited.containsKey(computationURI)){
				caller.initialiseClientSide(this);
				caller.getClientSideReference().acceptResult(computationURI, null);
				caller.cleanUpClientSide();
				return;
			}
			
			visited.put(computationURI, true);
			(server_edp.getContentAccessEndpoint().getClientSideReference()).remove(computationURI, key, caller);
		}
	}
    
    @SuppressWarnings("unchecked")
	public <R extends Serializable> void map(String computationURI, SelectorI selector, ProcessorI<R> processor) throws Exception {
		if (computationURI == null || computationURI.isEmpty() || selector == null || processor == null) 
	        throw new IllegalArgumentException("Parametre(s) de mapSync null "); 
		
		this.traceMessage("----MAP------\n");
		
		if (visitedMap.containsKey(computationURI)) return;
		visitedMap.put(computationURI, true);
		
        Stream<ContentDataI> mapResults = (Stream<ContentDataI>) table.values().stream()
        		.filter(selector)
        		.map(processor);
        streamMap.put(computationURI, mapResults);
        
        this.traceMessage("- Passe au noeud suivant\n");
        server_edp.getMapReduceEndpoint().getClientSideReference().map(computationURI, selector, processor);
	}

	@SuppressWarnings("unchecked")
	public <CI extends MapReduceResultReceptionCI, A extends Serializable, R> void reduce(
			String computationURI, ReductorI<A, R> reductor, 
			CombinatorI<A> combinator, A currentAcc, EndPointI<CI> caller) throws Exception {
		this.traceMessage("----REDUCE------\n");

		
		if (computationURI == null || computationURI.isEmpty() || reductor == null || combinator == null || caller == null || currentAcc == null) {
	        throw new IllegalArgumentException("Parametre(s) de reduceSync null ");    
		}
		
		if (visitedReduce.containsKey(computationURI)) {
			caller.initialiseClientSide(this);
			this.traceMessage("- ACCEPT\n");
			caller.getClientSideReference().acceptResult(computationURI, getURI(), currentAcc);	
			caller.cleanUpClientSide();
			return;
		}
		visitedReduce.put(computationURI, true);
		
		ReductorI<A, ContentDataI> reduct = (ReductorI<A, ContentDataI>) reductor;
		Stream<ContentDataI> mapResults; 
		
		mapResults = streamMap.get(computationURI);
		
		if (mapResults == null)
			throw new IllegalStateException("Pas de resultats trouvé pour computationUri: " + computationURI);
		
		A reduceResult = mapResults.reduce(currentAcc, reduct, combinator);
		this.traceMessage("- Passe au noeud suivant\n");
		server_edp.getMapReduceEndpoint().getClientSideReference().reduce(computationURI, reductor, combinator, currentAcc, reduceResult, caller);
	}
    
    
    //Méthodes Synchrones
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

	public void clearComputation(String computationURI) throws Exception {
		if (computationURI == null || computationURI.isEmpty() )
			throw new IllegalArgumentException("ComputationURI null");
		
		if (visited.containsKey(computationURI)) {
			visited.remove(computationURI);
			(server_edp.getContentAccessEndpoint().getClientSideReference()).clearComputation(computationURI);
		}
	}
	
	public void clearMapReduceComputation(String computationURI) throws Exception {
		if (computationURI == null || computationURI.isEmpty() )
			throw new IllegalArgumentException("ComputationURI null");
		
		if (visitedMap.containsKey(computationURI) && visitedReduce.containsKey(computationURI)){
				streamMap.remove(computationURI);
				visitedMap.remove(computationURI);
				visitedReduce.remove(computationURI);
			server_edp.getMapReduceEndpoint().getClientSideReference().clearMapReduceComputation(computationURI);
		}
	}
	
	@SuppressWarnings("unchecked")
	public <R extends Serializable> void mapSync(String computationURI, SelectorI selector, ProcessorI<R> processor) throws Exception {
		if (computationURI == null || computationURI.isEmpty() || selector == null || processor == null) 
	        throw new IllegalArgumentException("Parametre(s) de mapSync null ");    		
		
		synchronized (visitedMap) {
			if (visitedMap.containsKey(computationURI)) return;
			visitedMap.put(computationURI, true);
		}
		
        Stream<ContentDataI> mapResults = (Stream<ContentDataI>) table.values().stream()
        		.filter(selector)
        		.map(processor);
        
        streamMap.put(computationURI, mapResults);
        
        server_edp.getMapReduceEndpoint().getClientSideReference().mapSync(computationURI, selector, processor);
	}

	@SuppressWarnings("unchecked")
	public <A extends Serializable, R> A reduceSync(String computationURI, ReductorI<A, R> reductor, CombinatorI<A> combinator, A currentAcc)
			throws Exception {
		if (computationURI == null || computationURI.isEmpty() || reductor == null || combinator == null) {
	        throw new IllegalArgumentException("Parametre(s) de reduceSync null ");    
		}
		
		synchronized (visitedReduce) {
			if (visitedReduce.containsKey(computationURI)) return currentAcc;		
			visitedReduce.put(computationURI, true);
		}
		
		ReductorI<A, ContentDataI> reduct = (ReductorI<A, ContentDataI>) reductor;
		Stream<ContentDataI> mapResults; 
		
		synchronized (streamMap) {
			mapResults = streamMap.get(computationURI);
		}
		
		if (mapResults == null)
			throw new IllegalStateException("Pas de resultats trouvé pour computationUri: " + computationURI);
		
		A reduceResult = mapResults.reduce(currentAcc, reduct, combinator);
		
		reduceResult = server_edp.getMapReduceEndpoint().getClientSideReference().reduceSync(computationURI, reductor, combinator, reduceResult);

		return reduceResult;
	}
}
