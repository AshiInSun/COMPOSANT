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
import fr.sorbonne_u.cps.dht_mapreduce.interfaces.endpoints.ContentNodeCompositeEndPointI;
import fr.sorbonne_u.cps.dht_mapreduce.interfaces.frontend.DHTServicesCI;
import fr.sorbonne_u.cps.dht_mapreduce.interfaces.management.DHTManagementCI;
import fr.sorbonne_u.cps.dht_mapreduce.interfaces.mapreduce.MapReduceSyncCI;
import fr.sorbonne_u.cps.dht_mapreduce.interfaces.mapreduce.ParallelMapReduceCI;
import fr.sorbonne_u.cps.dht_mapreduce.interfaces.mapreduce.CombinatorI;
import fr.sorbonne_u.cps.dht_mapreduce.interfaces.mapreduce.ProcessorI;
import fr.sorbonne_u.cps.dht_mapreduce.interfaces.mapreduce.ReductorI;
import fr.sorbonne_u.cps.dht_mapreduce.interfaces.mapreduce.SelectorI;
import fr.sorbonne_u.cps.dht_mapreduce.interfaces.mapreduce.ParallelMapReduceI.ParallelismPolicyI;
import fr.sorbonne_u.cps.mapreduce.utils.IntInterval;
import fr.sorbonne_u.cps.mapreduce.utils.SerializablePair;
import fr.sorbonne_u.cps.dht_mapreduce.interfaces.mapreduce.MapReduceCI;
import fr.sorbonne_u.cps.dht_mapreduce.interfaces.mapreduce.MapReduceResultReceptionCI;
import fr.sorbonne_u.cps.dht_mapreduce.interfaces.content.ContentAccessCI;


import java.io.Serializable;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.locks.ReentrantReadWriteLock;
import java.util.function.Function;
import java.util.stream.Collectors;
import java.util.stream.Stream;

import defaultTeam.endpoints.BCMAsyncContentNodeCompositeEndPoint;

@OfferedInterfaces(offered = {ContentAccessSyncCI.class, MapReduceSyncCI.class, 
        ContentAccessCI.class, MapReduceCI.class, DHTServicesCI.class, 
        ResultReceptionCI.class, MapReduceResultReceptionCI.class, ParallelMapReduceCI.class, 
        DHTManagementCI.class})
@RequiredInterfaces(required = {ContentAccessSyncCI.class, MapReduceSyncCI.class, 
        ContentAccessCI.class, MapReduceCI.class, ParallelMapReduceCI.class, 
        ResultReceptionCI.class, MapReduceResultReceptionCI.class,
        DHTManagementCI.class})
public class NodeAsyncComponent extends AbstractComponent {
	
	private IntInterval interval;
	private String uri;
	public static final String CONTENT_ACCESS_HANDLER_URI = "caah";
	public static final String MAP_REDUCE_HANDLER_URI = "mrah";
	public int NB_NODES;
	
	protected List<BCMAsyncContentNodeCompositeEndPoint> fingers;
	protected List<Integer> fingersOffsets;
	protected List<SerializablePair<
    ContentNodeCompositeEndPointI<ContentAccessCI, ParallelMapReduceCI, DHTManagementCI>,
    Integer>>
	fingerTable;
	
    private final Map<ContentKeyI, ContentDataI> table;
    //HashMap<String,Stream<ContentDataI>> streamMap;
    private final Map<String, List<Object>> mapResults;
    protected final ConcurrentHashMap<String, CompletableFuture<Boolean>> isMapDone
    	= new ConcurrentHashMap<>();
    private final java.util.concurrent.Semaphore endpointLock = new java.util.concurrent.Semaphore(1);
    //en mode fair: pour éviter la famine des op de management 
    private final ReentrantReadWriteLock globalLock = new ReentrantReadWriteLock(true); 
    
    private Map<String, Boolean> visited;
    private Map<String, Boolean> visitedMap;	// On peut optimiser ces deux hashmap visited pour map reduce
    private Map<String, Boolean> visitedReduce;
    
    BCMAsyncContentNodeCompositeEndPoint client_edp; //me
    BCMAsyncContentNodeCompositeEndPoint server_edp; //the next
    BCMAsyncContentNodeCompositeEndPoint dht_edp; //only for the first node : connexion to facade
    
    protected NodeAsyncComponent(String uri, int debut, int fin,
		BCMAsyncContentNodeCompositeEndPoint dht_edp,
		BCMAsyncContentNodeCompositeEndPoint client_edp,
		BCMAsyncContentNodeCompositeEndPoint server_edp,
		int NB_NODES) throws Exception {
    	
        super(1, 0);

        this.interval = new IntInterval(debut, fin);
        this.uri = uri;
        this.table = new ConcurrentHashMap<>();
        this.mapResults = new ConcurrentHashMap<>();
        this.visited = new HashMap<>();
        this.visitedMap = new HashMap<>();
        this.visitedReduce = new HashMap<>();
        this.client_edp = client_edp;
        this.server_edp = server_edp;
        this.fingerTable = new ArrayList<>(4); 
        for (int i = 0; i < 4; i++) {
            fingerTable.add(null);
        }

        if(debut==0) {
        	this.dht_edp = dht_edp;
        }else {
        	this.dht_edp = null;
        }
        
        this.createNewExecutorService(CONTENT_ACCESS_HANDLER_URI, 4,false);
        this.createNewExecutorService(MAP_REDUCE_HANDLER_URI, 4,false);
        
        client_edp.initialiseServerSide(this);
        if(debut==0) {
        	dht_edp.initialiseServerSide(this);
        }
    }
    @Override
    public void start() throws ComponentStartException {
    	this.traceMessage("Noeud " + uri + " lancé.\n");
    	try {
			server_edp.initialiseClientSide(this);
		} catch (ConnectionException e) {
			e.printStackTrace();
		}
    	this.traceMessage("\n");
        super.start();
    }
    
    @Override
    public void finalise() throws Exception {
    	server_edp.cleanUpClientSide();
    	super.finalise();
    }

    @Override
    public void shutdown() throws ComponentShutdownException {
    	for(SerializablePair<ContentNodeCompositeEndPointI<ContentAccessCI, ParallelMapReduceCI, DHTManagementCI>, Integer>
    	fingerInfo : fingerTable) {
    		fingerInfo.first().cleanUpClientSide();
    	}
        if(interval.first() == 0) {
        	dht_edp.cleanUpServerSide();
        }
        client_edp.cleanUpServerSide();
        super.shutdown();
    }
    
    public boolean existStream(String computationURI) {
		return mapResults.containsKey(computationURI);
	}
    
    public String getURI() {
    	return this.uri;
    }
    
    //Méthodes de Management
    public void computeChords(String computationURI, int numberOfChords) throws Exception {
    	if(numberOfChords!=0) {
	    	List<SerializablePair<
	        ContentNodeCompositeEndPointI<ContentAccessCI, ParallelMapReduceCI, DHTManagementCI>,
	        Integer>>
	    	tempTable = new ArrayList<>(numberOfChords-1); 
	    	for(int i = 0; i <= 4 && (1 << i) < numberOfChords; i++) {
	    		int e = (1<<i);
	    		
	    		SerializablePair<
	            ContentNodeCompositeEndPointI<ContentAccessCI, ParallelMapReduceCI, DHTManagementCI>,
	            Integer> pairinfo = getChordInfo(e);
	    		pairinfo.first().initialiseClientSide(this);
	    		tempTable.add(pairinfo);
	    	}
	    	this.fingerTable = tempTable;
	    	try {
	            server_edp.getDHTManagementEndpoint().getClientSideReference()
	                .computeChords(computationURI, numberOfChords-1);
	        } catch (Exception e) {
	            this.traceMessage("Erreur lors de la propagation de computeChords : " + e.getMessage() + "\n");
	        }
    	}else {
    		//
    	}
    }
    
	public SerializablePair<
	    ContentNodeCompositeEndPointI<ContentAccessCI,
	        ParallelMapReduceCI,DHTManagementCI>,
	    Integer> getChordInfo(int offset) throws Exception {
		    if (offset == 0) {
		    	return new SerializablePair<
			    	    ContentNodeCompositeEndPointI<
			    	        ContentAccessCI,
			    	        ParallelMapReduceCI,
			    	        DHTManagementCI>,
			    	    Integer
			    	>(
			    	    (ContentNodeCompositeEndPointI<ContentAccessCI, ParallelMapReduceCI, DHTManagementCI>) (client_edp.copyWithSharable()),
			    	    interval.first()
			    	);
		    }else{
		    	return (server_edp.getDHTManagementEndpoint().getClientSideReference().getChordInfo(offset-1));
		    }
	}
    
    //Méthodes Asynchrones
    public <CI extends ResultReceptionCI> void get(
    		String computationURI, ContentKeyI key, EndPointI<CI> caller) throws Exception {
    
    		int h = key.hashCode();
    		this.traceMessage(this.uri +" uri || comput : "+computationURI + " || apell a get()\n");
		
			if ( interval.in(h) ) {
				endpointLock.acquire();
				ContentDataI result = table.get(key);
				try {
					caller.initialiseClientSide(this);
			        caller.getClientSideReference().acceptResult(computationURI, result);
			        this.traceMessage(computationURI + " || result :" + result + "not supposed to be null\n");
			        caller.cleanUpClientSide();
		        } finally {
		            endpointLock.release();  
		        }
			}
			else {
				if (visited.containsKey(computationURI)) {
					endpointLock.acquire();
					try {
						caller.initialiseClientSide(this);
				        caller.getClientSideReference().acceptResult(computationURI, null);
				        this.traceMessage(computationURI + " || all seen, not in the table\n");
				        caller.cleanUpClientSide();
			        } finally {
			            endpointLock.release();  
			        }
					return;
				}
				BCMAsyncContentNodeCompositeEndPoint temp = null;
				int tempDist = Integer.MAX_VALUE;
				for(SerializablePair<
		                ContentNodeCompositeEndPointI<ContentAccessCI, ParallelMapReduceCI, DHTManagementCI>,
		                Integer> fingerInfo : fingerTable) {
					if (fingerInfo == null) continue;
					int dist = h - fingerInfo.second();
					
					if (dist >= 0 && dist < tempDist) {
		                tempDist = dist;
		                temp = (BCMAsyncContentNodeCompositeEndPoint) fingerInfo.first();
		            }
				}
				
				if(temp==null) {
					temp=server_edp;
				}
				visited.put(computationURI, true);
				(temp.getContentAccessEndpoint()).getClientSideReference().get(computationURI, key, caller.copyWithSharable());
			}
    }
    
    public <CI extends ResultReceptionCI> void put(
    		String computationURI, ContentKeyI key, ContentDataI value, EndPointI<CI> caller) throws Exception {

			int h = key.hashCode();
			
			if ( interval.in(h) ) {
				endpointLock.acquire();
				ContentDataI result =  table.put(key, value);
				this.traceMessage(computationURI + "|| put - key, value : " + key + ", " + value + "\n");
				try {
					caller.initialiseClientSide(this);
			        caller.getClientSideReference().acceptResult(computationURI, result);
			        caller.cleanUpClientSide();
		        } finally {
		            endpointLock.release();  
		        }
		        return;
			}
			else {
				if (visited.containsKey(computationURI)){
					endpointLock.acquire();
					try {
						caller.initialiseClientSide(this);
				        caller.getClientSideReference().acceptResult(computationURI, null);
				        caller.cleanUpClientSide();
			        } finally {
			            endpointLock.release();  
			        }
					return;
				}
				BCMAsyncContentNodeCompositeEndPoint temp = null;
				int tempDist = Integer.MAX_VALUE;
				for(SerializablePair<
		                ContentNodeCompositeEndPointI<ContentAccessCI, ParallelMapReduceCI, DHTManagementCI>,
		                Integer> fingerInfo : fingerTable) {
					if (fingerInfo == null) continue;
					int dist = h - fingerInfo.second();
					
					if (dist >= 0 && dist < tempDist) {
		                tempDist = dist;
		                temp = (BCMAsyncContentNodeCompositeEndPoint) fingerInfo.first();
		            }
				}
				
				if(temp==null) {
					temp=server_edp;
				}
				visited.put(computationURI, true);
				(temp.getContentAccessEndpoint().getClientSideReference()).put(computationURI, key, value, caller.copyWithSharable());	
			}
	}
    public <CI extends ResultReceptionCI> void remove(String computationURI, ContentKeyI key, EndPointI<CI> caller) throws Exception {
		
	    	int h = key.hashCode();
			
			if ( interval.in(h) ) {
				endpointLock.acquire();
				ContentDataI result =  table.remove(key);
				try {
				caller.initialiseClientSide(this);
		        caller.getClientSideReference().acceptResult(computationURI, result);
		        caller.cleanUpClientSide();				
				} finally {
					endpointLock.release();
				}
			}
			else {
				if (visited.containsKey(computationURI)){
					endpointLock.acquire();
					try {
					caller.initialiseClientSide(this);
					caller.getClientSideReference().acceptResult(computationURI, null);
					caller.cleanUpClientSide();				
					} finally {
						endpointLock.release();
					}
					return;
				}
				BCMAsyncContentNodeCompositeEndPoint temp = null;
				int tempDist = Integer.MAX_VALUE;
				for(SerializablePair<
		                ContentNodeCompositeEndPointI<ContentAccessCI, ParallelMapReduceCI, DHTManagementCI>,
		                Integer> fingerInfo : fingerTable) {
					if (fingerInfo == null) continue;
					int dist = h - fingerInfo.second();
					
					if (dist >= 0 && dist < tempDist) {
		                tempDist = dist;
		                temp = (BCMAsyncContentNodeCompositeEndPoint) fingerInfo.first();
		                
		            }
				}
				
				if(temp==null) {
					temp=server_edp;
				}
				visited.put(computationURI, true);
				(temp.getContentAccessEndpoint().getClientSideReference()).remove(computationURI, key, caller.copyWithSharable());
			}
	}
    
    @SuppressWarnings("unchecked")
	public <R extends Serializable> void map(String computationURI, SelectorI selector, ProcessorI<R> processor) throws Exception {

    	assert computationURI != null && !computationURI.isEmpty() && selector != null && processor != null :
    		"Parametre(s) de map non valides";
		
		this.traceMessage("Execute map...\n");
		CompletableFuture<Boolean> cfuture = new CompletableFuture<>();
		isMapDone.putIfAbsent(computationURI, cfuture);
		
		if (visitedMap.containsKey(computationURI)) return;
		visitedMap.put(computationURI, true);
		
        /**Stream<ContentDataI> mapResults = (Stream<ContentDataI>) table.values().stream()
        		.filter(selector)
        		.map(processor);
        streamMap.put(computationURI, mapResults);**/
		List<Object> results = table.values().stream()
                .filter(selector)
                .map(data ->  processor.apply(data))
                .collect(Collectors.toList());
		mapResults.put(computationURI, results);
        
        cfuture = isMapDone.get(computationURI);
        if(cfuture!=null) {
        	cfuture.complete(true);
        }else {
        	throw new Exception("No pending request for isMapDone" + computationURI);
        }
        this.traceMessage("- Passe au noeud suivant\n");
        server_edp.getMapReduceEndpoint().getClientSideReference().map(computationURI, selector, processor);
	}

	@SuppressWarnings("unchecked")
	public <CI extends MapReduceResultReceptionCI, A extends Serializable, R> void reduce(
			String computationURI, ReductorI<A, R> reductor, 
			CombinatorI<A> combinator, A currentAcc, EndPointI<CI> caller) throws Exception {

		assert computationURI != null && !computationURI.isEmpty() && reductor != null && combinator != null && caller != null :
    		"Parametre(s) de reduce non valides";
		this.traceMessage("Reduce waiting for map...\n");	
		CompletableFuture<Boolean> cfuture = new CompletableFuture<>();
		isMapDone.putIfAbsent(computationURI, cfuture);
		cfuture = isMapDone.get(computationURI);
		cfuture.get();
		this.traceMessage("Execute reduce...\n");

		if (visitedReduce.containsKey(computationURI)) {
			endpointLock.acquire();
			try {
			caller.initialiseClientSide(this);
			this.traceMessage("- Appel acceptResult\n");
			caller.getClientSideReference().acceptResult(computationURI, getURI(), currentAcc);	
			caller.cleanUpClientSide();
			}finally {
				endpointLock.release();
			}
			return;
		}
		visitedReduce.put(computationURI, true);
		
		List<Object> values = mapResults.get(computationURI);
        if (values == null)
            throw new IllegalStateException("Pas de resultats pour " + computationURI);

        Stream<R> stream = values.stream().map(d -> (R) d);
	
		if (mapResults == null)
			throw new IllegalStateException("Pas de resultats trouvé pour computationUri: " + computationURI);
		
		A reduced = stream.reduce(currentAcc, reductor, combinator);
		this.traceMessage("- Passe au noeud suivant\n");
		server_edp.getMapReduceEndpoint().getClientSideReference().reduce(computationURI, reductor, combinator, currentAcc, reduced, caller.copyWithSharable());
	}
	
	//Methodes MapReduce Parallel
	public <R extends Serializable> void parallelMap(String computationURI, SelectorI selector, ProcessorI<R> processor,
			ParallelismPolicyI parallelismPolicy) throws Exception {
		assert computationURI != null && !computationURI.isEmpty() && selector != null && processor != null :
    		"Parametre(s) de map non valides";
		this.traceMessage("Execute map...\n");
		CompletableFuture<Boolean> cfuture = new CompletableFuture<>();
		isMapDone.putIfAbsent(computationURI, cfuture);
		
		synchronized (visitedMap) {
	        if (visitedMap.containsKey(computationURI)) return;
	        visitedMap.put(computationURI, true);
	    }
		if (parallelismPolicy == null || ((AllNodesPolicy) parallelismPolicy).apply(getURI())) {
	        this.traceMessage("Executing local map on " + getURI() + "\n");
	        
			List<Object> results = table.values().stream()
	                .filter(selector)
	                .map(data ->  processor.apply(data))
	                .collect(Collectors.toList());
			mapResults.put(computationURI, results);
	        
	        cfuture = isMapDone.get(computationURI);
	        if(cfuture!=null) {
	        	cfuture.complete(true);
	        }else {
	        	throw new Exception("No pending request for isMapDone" + computationURI);
	        }
		}
		BCMAsyncContentNodeCompositeEndPoint temp = null;
		for(SerializablePair<
                ContentNodeCompositeEndPointI<ContentAccessCI, ParallelMapReduceCI, DHTManagementCI>,
                Integer> fingerInfo : fingerTable) {
			if (fingerInfo == null) continue;
            temp = (BCMAsyncContentNodeCompositeEndPoint) fingerInfo.first();
            temp.getMapReduceEndpoint().getClientSideReference().parallelMap(computationURI, selector, processor, parallelismPolicy);
		}
		
		if(temp==null) {
			temp=server_edp;
			temp.getMapReduceEndpoint().getClientSideReference().parallelMap(computationURI, selector, processor, parallelismPolicy);
		}
		visited.put(computationURI, true);
        this.traceMessage("- Passe au noeud suivant\n");
        
	}
	
	public <A extends Serializable, R, I extends MapReduceResultReceptionCI> void parallelReduce(String computationURI,
			ReductorI<A, R> reductor, CombinatorI<A> combinator, A identityAcc, A currentAcc,
			ParallelismPolicyI parallelismPolicy, EndPointI<I> caller) throws Exception {
		assert computationURI != null && !computationURI.isEmpty() && reductor != null && combinator != null && caller != null :
    		"Parametre(s) de reduce non valides";
		this.traceMessage("Reduce waiting for map...\n");	
		CompletableFuture<Boolean> cfuture = new CompletableFuture<>();
		isMapDone.putIfAbsent(computationURI, cfuture);
		cfuture = isMapDone.get(computationURI);
		cfuture.get();
		this.traceMessage("Execute reduce...\n");
		
		synchronized (visitedReduce) {
	        if (visitedReduce.containsKey(computationURI)) return;
	        visitedReduce.put(computationURI, true);
	    }
		
		//Application Logique
		List<Object> values = mapResults.get(computationURI);
        if (values == null)
            return;

        Stream<R> stream = values.stream().map(d -> (R) d);
	
		if (mapResults == null)
			throw new IllegalStateException("Pas de resultats trouvé pour computationUri: " + computationURI);
		A reduced = stream.reduce(currentAcc, reductor, combinator);
		
		endpointLock.acquire();
		try {
	        this.traceMessage("[" + getURI() + "] Envoi de l'accumulateur local à la façade.\n");
	        caller.initialiseClientSide(this);
	        caller.getClientSideReference().acceptResult(computationURI, getURI(), reduced);
	        caller.cleanUpClientSide();
	    } finally {
	        this.traceMessage("[" + getURI() + "] Échec de l'envoi du résultat \n" );
	        endpointLock.release();
	    }
		
		BCMAsyncContentNodeCompositeEndPoint temp = null;
		for(SerializablePair<
                ContentNodeCompositeEndPointI<ContentAccessCI, ParallelMapReduceCI, DHTManagementCI>,
                Integer> fingerInfo : fingerTable) {
			if (fingerInfo == null) continue;
            temp = (BCMAsyncContentNodeCompositeEndPoint) fingerInfo.first();
            temp.getMapReduceEndpoint().getClientSideReference().parallelReduce(computationURI, reductor, combinator, identityAcc, identityAcc, parallelismPolicy, caller.copyWithSharable());
		}
		
		if(temp==null) {
			temp=server_edp;
			temp.getMapReduceEndpoint().getClientSideReference().parallelReduce(computationURI, reductor, combinator, identityAcc, identityAcc, parallelismPolicy, caller.copyWithSharable());
		}
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
		
		assert computationURI != null && !computationURI.isEmpty() :
    		"computationURI vide dans clearComputation";
		
		if (visited.containsKey(computationURI)) {
			visited.remove(computationURI);
			(server_edp.getContentAccessEndpoint().getClientSideReference()).clearComputation(computationURI);
		}
	}
	
	public void clearMapReduceComputation(String computationURI) throws Exception {

		assert computationURI != null && !computationURI.isEmpty() :
    		"computationURI vide dans clearMapReduceComputation";
		
		if (visitedMap.containsKey(computationURI) && visitedReduce.containsKey(computationURI)){
				mapResults.remove(computationURI);
				visitedMap.remove(computationURI);
				visitedReduce.remove(computationURI);
			server_edp.getMapReduceEndpoint().getClientSideReference().clearMapReduceComputation(computationURI);
		}
	}
	
	@SuppressWarnings("unchecked")
    public <R extends Serializable> void mapSync(String computationURI, SelectorI selector, ProcessorI<R> processor) throws Exception {
        if (visitedMap.putIfAbsent(computationURI, true) != null) return;

        List<Object> results = table.values().stream()
                .filter(selector)
                .map(data ->processor.apply(data))
                .collect(Collectors.toList());

        mapResults.put(computationURI, results);

        server_edp.getMapReduceEndpoint().getClientSideReference().mapSync(computationURI, selector, processor);
    }

    @SuppressWarnings("unchecked")
    public <A extends Serializable, R> A reduceSync(String computationURI, ReductorI<A, R> reductor,
                                                    CombinatorI<A> combinator, A currentAcc) throws Exception {
        if (visitedReduce.putIfAbsent(computationURI, true) != null) return currentAcc;

        List<Object> values = mapResults.get(computationURI);
        if (values == null)
            throw new IllegalStateException("Pas de resultats pour " + computationURI);

        Stream<R> stream = values.stream().map(d -> (R) d);
        A reduced = stream.reduce(currentAcc, reductor, combinator);

        return server_edp.getMapReduceEndpoint().getClientSideReference()
                .reduceSync(computationURI, reductor, combinator, reduced);
    }
}
