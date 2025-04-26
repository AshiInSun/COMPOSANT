package defaultTeam;
import java.io.Serializable;
import fr.sorbonne_u.cps.dht_mapreduce.interfaces.mapreduce.MapReduceResultReceptionCI;

import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.locks.ReentrantReadWriteLock;

import defaultTeam.endpoints.BCMAsyncContentNodeCompositeEndPoint;
import defaultTeam.endpoints.ConcreteBCMEndPoint;
import defaultTeam.endpoints.DHTServicesEndPoint;
import defaultTeam.endpoints.MapReduceResultEndPoint;
import defaultTeam.endpoints.ResultEndPoint;
import defaultTeam.utils.AllNodesPolicy;
import defaultTeam.utils.LoadPolicy;
import defaultTeam.utils.ValidChordPolicy;
import fr.sorbonne_u.components.AbstractComponent;
import fr.sorbonne_u.components.annotations.OfferedInterfaces;
import fr.sorbonne_u.components.annotations.RequiredInterfaces;
import fr.sorbonne_u.components.endpoints.EndPointI;
import fr.sorbonne_u.components.exceptions.ComponentShutdownException;
import fr.sorbonne_u.components.exceptions.ComponentStartException;
import fr.sorbonne_u.components.exceptions.ConnectionException;
import fr.sorbonne_u.cps.dht_mapreduce.interfaces.content.ContentAccessCI;
import fr.sorbonne_u.cps.dht_mapreduce.interfaces.content.ContentAccessSyncCI;
import fr.sorbonne_u.cps.dht_mapreduce.interfaces.content.ContentDataI;
import fr.sorbonne_u.cps.dht_mapreduce.interfaces.content.ContentKeyI;
import fr.sorbonne_u.cps.dht_mapreduce.interfaces.content.ResultReceptionCI;
import fr.sorbonne_u.cps.dht_mapreduce.interfaces.frontend.DHTServicesCI;
import fr.sorbonne_u.cps.dht_mapreduce.interfaces.management.DHTManagementCI;
import fr.sorbonne_u.cps.dht_mapreduce.interfaces.management.LoadPolicyI;
import fr.sorbonne_u.cps.dht_mapreduce.interfaces.mapreduce.CombinatorI;
import fr.sorbonne_u.cps.dht_mapreduce.interfaces.mapreduce.MapReduceCI;
import fr.sorbonne_u.cps.dht_mapreduce.interfaces.mapreduce.MapReduceSyncCI;
import fr.sorbonne_u.cps.dht_mapreduce.interfaces.mapreduce.ProcessorI;
import fr.sorbonne_u.cps.dht_mapreduce.interfaces.mapreduce.ReductorI;
import fr.sorbonne_u.cps.dht_mapreduce.interfaces.mapreduce.SelectorI;
import fr.sorbonne_u.cps.mapreduce.utils.URIGenerator;

@OfferedInterfaces(offered = {
		DHTServicesCI.class, ResultReceptionCI.class, MapReduceResultReceptionCI.class,
		DHTManagementCI.class, MapReduceCI.class, ContentAccessSyncCI.class})
@RequiredInterfaces(required = {
		DHTServicesCI.class, ContentAccessSyncCI.class, MapReduceSyncCI.class,
		MapReduceCI.class, ContentAccessCI.class, DHTManagementCI.class, ResultReceptionCI.class})
public class FacadeAsyncComponent extends AbstractComponent {

	private BCMAsyncContentNodeCompositeEndPoint server_edp;
	private DHTServicesEndPoint client_edp;
	protected final ResultEndPoint caller;
	protected final MapReduceResultEndPoint mapreduce_caller;
	protected final ConcurrentHashMap<String, CompletableFuture<ContentDataI>> pendingResults 
		= new ConcurrentHashMap<>();
	protected final ConcurrentHashMap<String, CompletableFuture<?>> pendingResultsMapReduce 
		= new ConcurrentHashMap<>();
	//Map pour le parrallel map reduce
	private final Map<String, List<Serializable>> partialResultsMapReduce = new ConcurrentHashMap<>();
	private final Map<String, Integer> accCount = new ConcurrentHashMap<>();
	private final Map<String, CombinatorI<Serializable>> combinators = new ConcurrentHashMap<>();
	private final Map<String, ReductorI<Serializable, Serializable>> reductors = new ConcurrentHashMap<>();
	private final Map<String, Serializable> identityAccumulators = new ConcurrentHashMap<>();
	//
	private final ReentrantReadWriteLock globalLock = new ReentrantReadWriteLock(true); 
	public static final String CONTENT_ACCESS_HANDLER_URI = "caah";
	public static final String MAP_REDUCE_HANDLER_URI = "mrah";
	public static final String ACCEPT_RESULT_HANDLER_URI = "arah";
	public static final String MANAGEMENT_HANDLER_URI = "mah";
	private int NB_NODES;
	private int globalPutCounter = 0;
	private static final int GLOBAL_SPLIT_THRESHOLD = 144;
	private final LoadPolicyI defaultPolicy = new LoadPolicy(90, 10);

    protected FacadeAsyncComponent(
    		String uri, DHTServicesEndPoint client_edp , 
    		BCMAsyncContentNodeCompositeEndPoint server_edp,
    		int NB_NODES) throws Exception {

		super(1, 0);
		//ENDPOINTS CALLERS
		String callerURI = URIGenerator.generateURI();
		String mapReduceCallerURI = URIGenerator.generateURI();
		this.NB_NODES = NB_NODES;
 		//Create new executor service....
		this.caller = new ResultEndPoint(callerURI);
		this.caller.initialiseServerSide(this);
		this.mapreduce_caller = new MapReduceResultEndPoint(mapReduceCallerURI);
		this.mapreduce_caller.initialiseServerSide(this);

		//OTHERS ENDPOINTS
        this.client_edp = client_edp;
        client_edp.initialiseServerSide(this);
        this.server_edp = server_edp;
        
        //PARRALELISM
        this.createNewExecutorService(CONTENT_ACCESS_HANDLER_URI, 4,true);
        this.createNewExecutorService(MAP_REDUCE_HANDLER_URI, 4,true);
        this.createNewExecutorService(ACCEPT_RESULT_HANDLER_URI, 4,true);
        this.createNewExecutorService(MANAGEMENT_HANDLER_URI, 1,true);
        this.traceMessage("FacadeAsyncComponent initialisé" );
    }

    @Override
    public void start() throws ComponentStartException {
    	try {
			server_edp.initialiseClientSide(this);
		} catch (ConnectionException e) {
			e.printStackTrace();
		}
        super.start();
        this.traceMessage("FacadeAsyncComponent démarré.");
    }
    @Override
    public void execute() throws Exception {
        super.execute();
        try {
        	String computationURI = URIGenerator.generateURI();
        	this.globalLock.writeLock().lock();
        	try {
        		this.computeChords(computationURI, NB_NODES);
        	}finally {
        		this.globalLock.writeLock().unlock();
        	}
		} catch (Exception e) {
			e.printStackTrace();
		}
        this.runTask(MANAGEMENT_HANDLER_URI, o -> {
            while (true) {
            	 try {
            		 
                     Thread.sleep(1000); // 5 secondes
                 } catch (InterruptedException e) {
                     this.traceMessage("⏹ Maintenance interrompue\n");
                     break;
                 }
            	 
                try {
                    String uri = URIGenerator.generateURI("AUTO_MAINT");
                    this.ComputeSplit(defaultPolicy, caller, NB_NODES);
                } catch (Exception e) {
                    this.traceMessage("⚠️ Erreur pendant maintenance : " + e.getMessage() + "\n");
                    e.printStackTrace();
                }    
            }
        });
    }

    @Override
    public void finalise() throws Exception {
    	server_edp.cleanUpClientSide();
    	caller.cleanUpServerSide();
    	mapreduce_caller.cleanUpServerSide();
        this.traceMessage("FacadeAsyncComponent se termine...");
        super.finalise();
    }

    @Override
    public void shutdown() throws ComponentShutdownException {
    	client_edp.cleanUpServerSide();
    	super.shutdown();
    }
    
    public void computeChords(String computationURI, int numberOfChords) throws Exception {
    	String computationURI_chord = URIGenerator.generateURI();
    	this.traceMessage("ComputeChordFromFacade...\n");
        server_edp.getDHTManagementEndpoint().getClientSideReference().computeChords(computationURI_chord, numberOfChords);
        this.traceMessage("ComputeChordEnded...\n");
	}
    public void split(String computationURI, LoadPolicyI loadPolicy, EndPointI<ResultReceptionCI> caller) throws Exception {
    	this.traceMessage("Split..\n");
		String computationURI_split = URIGenerator.generateURI();
		CompletableFuture<ContentDataI> cfuture = new CompletableFuture<>();
		pendingResults.put(computationURI_split, cfuture);
		server_edp.getDHTManagementEndpoint().getClientSideReference()
			.split(computationURI_split, defaultPolicy, caller.copyWithSharable());
		ContentDataI res = cfuture.get();
        this.traceMessage("Split ended\n");
	}
    public void merge(String computationURI, LoadPolicyI loadPolicy, EndPointI<ResultReceptionCI> caller) throws Exception {
    	this.traceMessage("Merge..\n");
		String computationURI_merge = URIGenerator.generateURI();
		CompletableFuture<ContentDataI> cfuture = new CompletableFuture<>();
		pendingResults.put(computationURI_merge, cfuture);
		server_edp.getDHTManagementEndpoint().getClientSideReference()
			.merge(computationURI_merge, defaultPolicy, caller.copyWithSharable());
		this.traceMessage("Getting merge future\n");
		ContentDataI res = cfuture.get();
        this.traceMessage("Merge Ended\n");
	}
    public void ComputeSplit(LoadPolicyI loadPolicy, EndPointI<ResultReceptionCI> caller, int numberOfChords) throws Exception{
    	try {
			globalLock.writeLock().lock();
			traceMessage("⛏ Déclenchement du split en tâche de fond...\n");
			this.split("", loadPolicy, caller);
			traceMessage("⛏ tache de fond fini split...\n");
			traceMessage("⛏ Déclenchement du merge en tâche de fond...\n");
			this.merge("", loadPolicy, caller);	
			traceMessage("⛏ tache de fond fini merge...\n");
			this.computeChords("", numberOfChords);
		} finally {
			globalLock.writeLock().unlock();
		}
    }

	public <CI extends ResultReceptionCI>ContentDataI get(ContentKeyI key) throws Exception {
		globalLock.readLock().lock();
		try {
			String computationURI = URIGenerator.generateURI();
			CompletableFuture<ContentDataI> cfuture = new CompletableFuture<>();
			pendingResults.put(computationURI, cfuture);
			server_edp.getContentAccessEndpoint().getClientSideReference().get(computationURI, key, caller.copyWithSharable());
			
			ContentDataI res = cfuture.get();
			server_edp.getContentAccessEndpoint().getClientSideReference().clearComputation(computationURI);
			return res;
		}finally {
			globalLock.readLock().unlock();
		}
	}
	
	public <CI extends ResultReceptionCI> ContentDataI put(ContentKeyI key, ContentDataI value) throws Exception {
		// Exécution normale du put sous readLock
		globalLock.readLock().lock();
		try {
			globalPutCounter++;
			String computationURI = URIGenerator.generateURI();
			CompletableFuture<ContentDataI> cfuture = new CompletableFuture<>();
			pendingResults.put(computationURI, cfuture);
			server_edp.getContentAccessEndpoint().getClientSideReference().put(computationURI, key, value, caller);
			ContentDataI res = cfuture.get();
			server_edp.getContentAccessEndpoint().getClientSideReference().clearComputation(computationURI);
			return res;
		} finally {
			globalLock.readLock().unlock();
		}
	}

	
	public ContentDataI remove(ContentKeyI key) throws Exception {
		globalLock.readLock().lock();
		try {
			String computationURI = URIGenerator.generateURI();
			CompletableFuture<ContentDataI> cfuture = new CompletableFuture<>();
			pendingResults.put(computationURI, cfuture);
			server_edp.getContentAccessEndpoint().getClientSideReference().remove(computationURI, key, caller);
			ContentDataI res = cfuture.get();
			server_edp.getContentAccessEndpoint().getClientSideReference().clearComputation(computationURI);
			return res;
		}finally {
			globalLock.readLock().unlock();
		}
	}
	
	public <R extends Serializable, A extends Serializable, CI extends MapReduceResultReceptionCI> A mapReduce(
			SelectorI selector, 
			ProcessorI<R> processor,
			ReductorI<A, R> reductor, 
			CombinatorI<A> combinator, 
			A initialAcc) throws Exception {
				
		globalLock.readLock().lock();
		System.out.println("MapReduce - a l'interieur du verrou");
		
		try {
			if (selector == null || processor == null || reductor == null || combinator == null || initialAcc == null ) 
				throw new IllegalArgumentException("Parametre(s) de mapReduce null "); 
			
			String computationURI = URIGenerator.generateURI("MAP_REDUCE");
			CompletableFuture<A> cfuture = new CompletableFuture<>();
			
			pendingResultsMapReduce.put(computationURI, cfuture);
			combinators.put(computationURI, (CombinatorI<Serializable>) combinator);
			reductors.put(computationURI, (ReductorI<Serializable, Serializable>) reductor);
			identityAccumulators.put(computationURI, initialAcc);
			accCount.put(computationURI, 0);
			partialResultsMapReduce.put(computationURI, new ArrayList<>());
			
			AllNodesPolicy police = new AllNodesPolicy();
			List<Integer> list = new ArrayList<>();
			list.add(1);
			ValidChordPolicy police_v = new ValidChordPolicy(list);
			server_edp.getMapReduceEndpoint().getClientSideReference().parallelMap(computationURI, selector, processor, police);
			A identityAcc = initialAcc;
			server_edp.getMapReduceEndpoint().getClientSideReference().reduce(
					computationURI, reductor, combinator, initialAcc, identityAcc, mapreduce_caller.copyWithSharable());
			System.out.println("MapReduce - avant get sur Future");
			A res = cfuture.get();
			System.out.println("MapReduce - après get sur Future");
			server_edp.getMapReduceEndpoint().getClientSideReference().clearMapReduceComputation(computationURI);
			return (A) res;
		}finally {
			globalLock.readLock().unlock();
		}
	}
	
	public void acceptResult(String computationURI, Serializable result) throws Exception {
		
		CompletableFuture<ContentDataI> future = pendingResults.remove(computationURI);
        if (future != null) {
            future.complete((ContentDataI) result);
        } else {
            throw new Exception("No pending request found for computation URI: " + computationURI);
        }
	}
	public void acceptResult(String computationURI, String emitterId, Serializable acc) throws Exception {
		
		CompletableFuture<Serializable> future = 
				(CompletableFuture<Serializable>) pendingResultsMapReduce.remove(computationURI);
        if (future != null) {
            future.complete((Serializable) acc);
        } else {
            throw new Exception("No pending request found for computation URI: " + computationURI);
        }
	}
	public void acceptResult(String computationURI, String emitterId, Serializable acc, int troll) throws Exception {
		
			System.out.println("Accept result MapReduce");
			List<Serializable> partials = partialResultsMapReduce.get(computationURI);
	        if (partials == null) {
	            throw new Exception("No partial result list found for computation URI: " + computationURI);
	        }
	        partials.add(acc);
	        int count = accCount.computeIfPresent(computationURI, (k, v) -> v + 1);
			
	        this.traceMessage("[Facade] Reçu " + count + "/" + NB_NODES + " résultats pour " + computationURI + "\n");
	        
	        if (count == NB_NODES) {
	            this.traceMessage("[Facade] Tous les accumulateurs reçus pour " + computationURI + ". Réduction finale...\n");

	            // Réduction finale avec combinator
	            CombinatorI<Serializable> combinator = combinators.get(computationURI);
	            ReductorI<Serializable, Serializable> reductor = reductors.get(computationURI);
	            Serializable identityAcc = identityAccumulators.get(computationURI);
	            Serializable finalAcc = partials.stream()
	                .reduce(identityAcc, reductor, combinator);

	            // Complétion de la future
	            CompletableFuture<Serializable> future =
	                (CompletableFuture<Serializable>) pendingResultsMapReduce.remove(computationURI);
	            if (future != null) {
	                future.complete(finalAcc);
	            } else {
	                throw new Exception("No pending future found for computation URI: " + computationURI);
	            }

	            // Nettoyage
	            partialResultsMapReduce.remove(computationURI);
	            accCount.remove(computationURI);
	            combinators.remove(computationURI);
	            reductors.remove(computationURI);
	            identityAccumulators.remove(computationURI);

	            this.traceMessage("[Facade] Réduction finale terminée pour " + computationURI + ".\n");
	        }
	}
}