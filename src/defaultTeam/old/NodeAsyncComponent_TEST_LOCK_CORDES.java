package defaultTeam.old;
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
import java.util.stream.Collectors;
import java.util.stream.Stream;

import defaultTeam.endpoints.BCMAsyncContentNodeCompositeEndPoint;

@OfferedInterfaces(offered = {ContentAccessSyncCI.class, MapReduceSyncCI.class, 
        ContentAccessCI.class, MapReduceCI.class, DHTServicesCI.class, 
        ResultReceptionCI.class, MapReduceResultReceptionCI.class,
        DHTManagementCI.class})
@RequiredInterfaces(required = {ContentAccessSyncCI.class, MapReduceSyncCI.class, 
        ContentAccessCI.class, MapReduceCI.class, 
        ResultReceptionCI.class, MapReduceResultReceptionCI.class,
        DHTManagementCI.class})
public class NodeAsyncComponent_TEST_LOCK_CORDES extends AbstractComponent {
	
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
    
    protected NodeAsyncComponent_TEST_LOCK_CORDES(String uri, int debut, int fin,
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
        this.fingerTable = new ArrayList<>(NB_NODES-1); 
        for (int i = 0; i < NB_NODES - 1; i++) {
            fingerTable.add(null);
        }

        if(debut==0) {
        	System.out.print("here");
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
    	List<SerializablePair<
        ContentNodeCompositeEndPointI<ContentAccessCI, ParallelMapReduceCI, DHTManagementCI>,
        Integer>>
    	tempTable = new ArrayList<>(numberOfChords-1); 
    	for(int i =1; i<numberOfChords; i++) {
    		SerializablePair<
    	    ContentNodeCompositeEndPointI<
    	        ContentAccessCI,
    	        ParallelMapReduceCI,
    	        DHTManagementCI>,
    	    Integer> pairinfo = getChordInfo(i);
    		tempTable.add(pairinfo);
    	}
    	globalLock.writeLock().lock();
    	try {
    		System.out.println("here");
    	}finally {
    		globalLock.writeLock().unlock();
    	}
    }
    
	public SerializablePair<
	    ContentNodeCompositeEndPointI<
	        ContentAccessCI,
	        ParallelMapReduceCI,
	        DHTManagementCI>,
	    Integer> getChordInfo(int offset) throws Exception {
	
		globalLock.readLock().lock();
    	try {
		    if (offset == 0) {
		    	System.out.println(interval.first());
		    	return new SerializablePair<
			    	    ContentNodeCompositeEndPointI<
			    	        ContentAccessCI,
			    	        ParallelMapReduceCI,
			    	        DHTManagementCI>,
			    	    Integer
			    	>(
			    	    client_edp,
			    	    interval.first()
			    	);
		    }else{
		    	return (server_edp.getDHTManagementEndpoint().getClientSideReference().getChordInfo(offset-1));
		    }
    	}finally {
    		globalLock.readLock().unlock();
    	}
	}
    
    //Méthodes Asynchrones
    public <CI extends ResultReceptionCI> void get(
    		String computationURI, ContentKeyI key, EndPointI<CI> caller) throws Exception {
    	
    		globalLock.readLock().lock();
    		try {
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
				
				visited.put(computationURI, true);
				(server_edp.getContentAccessEndpoint()).getClientSideReference().get(computationURI, key, caller.copyWithSharable());
			}
    		}finally {
    			globalLock.readLock().unlock();
    		}
    }
    
    public <CI extends ResultReceptionCI> void put(
    		String computationURI, ContentKeyI key, ContentDataI value, EndPointI<CI> caller) throws Exception {
    	globalLock.readLock().lock();
		try {
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
				
				visited.put(computationURI, true);
				(server_edp.getContentAccessEndpoint().getClientSideReference()).put(computationURI, key, value, caller.copyWithSharable());
			}
		} finally {
            globalLock.readLock().unlock();;  
        }
	}
    public <CI extends ResultReceptionCI> void remove(String computationURI, ContentKeyI key, EndPointI<CI> caller) throws Exception {
		
    	globalLock.readLock().lock();
    	try {
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
				
				visited.put(computationURI, true);
				(server_edp.getContentAccessEndpoint().getClientSideReference()).remove(computationURI, key, caller.copyWithSharable());
			}
    	}finally {
    		globalLock.readLock().unlock();
    	}
	}
    
    @SuppressWarnings("unchecked")
	public <R extends Serializable> void map(String computationURI, SelectorI selector, ProcessorI<R> processor) throws Exception {

    	assert computationURI != null && !computationURI.isEmpty() && selector != null && processor != null :
    		"Parametre(s) de map non valides";
		
		this.traceMessage("Execute map...\n");
		globalLock.readLock().lock();
		try {
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
		}finally {
			globalLock.readLock().unlock();
		}
	}

	@SuppressWarnings("unchecked")
	public <CI extends MapReduceResultReceptionCI, A extends Serializable, R> void reduce(
			String computationURI, ReductorI<A, R> reductor, 
			CombinatorI<A> combinator, A currentAcc, EndPointI<CI> caller) throws Exception {

		assert computationURI != null && !computationURI.isEmpty() && reductor != null && combinator != null && caller != null :
    		"Parametre(s) de reduce non valides";
		this.traceMessage("Reduce waiting for map...\n");	
		globalLock.readLock().lock();
		try {
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
		}finally {
			globalLock.readLock().unlock();
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
