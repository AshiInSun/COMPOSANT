package defaultTeam;
import java.io.Serializable;
import fr.sorbonne_u.cps.dht_mapreduce.interfaces.mapreduce.MapReduceResultReceptionCI;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.ConcurrentHashMap;
import fr.sorbonne_u.components.AbstractComponent;
import fr.sorbonne_u.components.annotations.OfferedInterfaces;
import fr.sorbonne_u.components.annotations.RequiredInterfaces;
import fr.sorbonne_u.components.exceptions.ComponentShutdownException;
import fr.sorbonne_u.components.exceptions.ComponentStartException;
import fr.sorbonne_u.components.exceptions.ConnectionException;
import fr.sorbonne_u.cps.dht_mapreduce.interfaces.content.ContentAccessCI;
import fr.sorbonne_u.cps.dht_mapreduce.interfaces.content.ContentAccessSyncCI;
import fr.sorbonne_u.cps.dht_mapreduce.interfaces.content.ContentDataI;
import fr.sorbonne_u.cps.dht_mapreduce.interfaces.content.ContentKeyI;
import fr.sorbonne_u.cps.dht_mapreduce.interfaces.content.ResultReceptionCI;
import fr.sorbonne_u.cps.dht_mapreduce.interfaces.frontend.DHTServicesCI;
import fr.sorbonne_u.cps.dht_mapreduce.interfaces.mapreduce.CombinatorI;
import fr.sorbonne_u.cps.dht_mapreduce.interfaces.mapreduce.MapReduceCI;
import fr.sorbonne_u.cps.dht_mapreduce.interfaces.mapreduce.MapReduceSyncCI;
import fr.sorbonne_u.cps.dht_mapreduce.interfaces.mapreduce.ProcessorI;
import fr.sorbonne_u.cps.dht_mapreduce.interfaces.mapreduce.ReductorI;
import fr.sorbonne_u.cps.dht_mapreduce.interfaces.mapreduce.SelectorI;
import fr.sorbonne_u.cps.mapreduce.utils.URIGenerator;

@OfferedInterfaces(offered = {
		DHTServicesCI.class})
@RequiredInterfaces(required = {
		DHTServicesCI.class, ContentAccessSyncCI.class, MapReduceSyncCI.class,
		MapReduceCI.class, ContentAccessCI.class})
public class FacadeAsyncComponent extends AbstractComponent 
	implements ResultReceptionCI, MapReduceResultReceptionCI{

	private BCMAsyncContentNodeCompositeEndPoint server_edp;
	private ConcreteBCMEndPoint<DHTServicesCI> client_edp;
	protected final ResultEndPoint caller;
	protected final MapReduceResultEndPoint mapreduce_caller;
	protected final ConcurrentHashMap<String, CompletableFuture<ContentDataI>> pendingResults = new ConcurrentHashMap<>();
	protected final ConcurrentHashMap<String, CompletableFuture<?>> pendingResultsMapReduce = new ConcurrentHashMap<>();

    protected FacadeAsyncComponent(
    		String uri, ConcreteBCMEndPoint<DHTServicesCI> client_edp , 
    		BCMAsyncContentNodeCompositeEndPoint server_edp) throws Exception {

		super(1, 0);
		//ENDPOINTS CALLERS
		String callerURI = URIGenerator.generateURI();
		String mapReduceCallerURI = URIGenerator.generateURI();
 		//Create new executor service....
		this.caller = new ResultEndPoint(callerURI);
		this.caller.initialiseServerSide(this);
		this.mapreduce_caller = new MapReduceResultEndPoint(mapReduceCallerURI);
		this.mapreduce_caller.initialiseServerSide(this);
		
		//OTHERS ENDPOINTS
        this.client_edp = client_edp;
        client_edp.initialiseServerSide(this);
        this.server_edp = server_edp;
        
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
    public void finalise() throws Exception {
    	server_edp.cleanUpClientSide();
    	assert caller.clientSideClean();
    	caller.cleanUpServerSide();
        this.traceMessage("FacadeAsyncComponent se termine...");
        super.finalise();
    }

    @Override
    public void shutdown() throws ComponentShutdownException {
    	client_edp.cleanUpServerSide();
    	super.shutdown();
    }

	public <CI extends ResultReceptionCI>ContentDataI get(ContentKeyI key) throws Exception {
		String computationURI = URIGenerator.generateURI();
		CompletableFuture<ContentDataI> cfuture = new CompletableFuture<>();
		pendingResults.put(computationURI, cfuture);
		server_edp.getContentAccessEndpoint().getClientSideReference().get(computationURI, key, caller.copyWithSharable());
		
		ContentDataI res = cfuture.get();
		server_edp.getContentAccessEndpoint().getClientSideReference().clearComputation(computationURI);
		return res;
	}
	
	public <CI extends ResultReceptionCI>ContentDataI put(ContentKeyI key, ContentDataI value) throws Exception {
		String computationURI = URIGenerator.generateURI();
		CompletableFuture<ContentDataI> cfuture = new CompletableFuture<>();
		pendingResults.put(computationURI, cfuture);
		server_edp.getContentAccessEndpoint().getClientSideReference().put(computationURI, key, value, caller);
		ContentDataI res = cfuture.get();
		server_edp.getContentAccessEndpoint().getClientSideReference().clearComputation(computationURI);
		return res;
	}
	
	public ContentDataI remove(ContentKeyI key) throws Exception {
		String computationURI = URIGenerator.generateURI();
		CompletableFuture<ContentDataI> cfuture = new CompletableFuture<>();
		pendingResults.put(computationURI, cfuture);
		server_edp.getContentAccessEndpoint().getClientSideReference().remove(computationURI, key, caller);
		ContentDataI res = cfuture.get();
		server_edp.getContentAccessEndpoint().getClientSideReference().clearComputation(computationURI);
		return res;
	}
	
	public <R extends Serializable, A extends Serializable, CI extends MapReduceResultReceptionCI> A mapReduce(
			SelectorI selector, 
			ProcessorI<R> processor,
			ReductorI<A, R> reductor, 
			CombinatorI<A> combinator, 
			A initialAcc) throws Exception {
		if (selector == null || processor == null || reductor == null || combinator == null || initialAcc == null ) 
			throw new IllegalArgumentException("Parametre(s) de mapReduce null "); 
		
		String computationURI = URIGenerator.generateURI("MAP_REDUCE");
		CompletableFuture<A> cfuture = new CompletableFuture<>();
		pendingResultsMapReduce.put(computationURI, cfuture);
		server_edp.getMapReduceEndpoint().getClientSideReference().map(computationURI, selector, processor);
		A identityAcc = initialAcc;
		server_edp.getMapReduceEndpoint().getClientSideReference().reduce(
				computationURI, reductor, combinator, initialAcc, identityAcc,mapreduce_caller.copyWithSharable()
				);
		A res = cfuture.get();
		server_edp.getMapReduceEndpoint().getClientSideReference().clearMapReduceComputation(computationURI);		
		return (A) res;
	}

	@Override
	public void acceptResult(String computationURI, Serializable result) throws Exception {
		CompletableFuture<ContentDataI> future = pendingResults.remove(computationURI);
        if (future != null) {
            future.complete((ContentDataI) result);
        } else {
            throw new Exception("No pending request found for computation URI: " + computationURI);
        }
	}
	@Override
	public void acceptResult(String computationURI, String emitterId, Serializable acc) throws Exception {
		CompletableFuture<Serializable> future = (CompletableFuture<Serializable>) pendingResultsMapReduce.remove(computationURI);
        if (future != null) {
            future.complete((Serializable) acc);
        } else {
            throw new Exception("No pending request found for computation URI: " + computationURI);
        }
	}
	
}