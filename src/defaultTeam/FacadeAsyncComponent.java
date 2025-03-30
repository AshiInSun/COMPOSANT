package defaultTeam;
import java.io.Serializable;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.Future;

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
	implements ResultReceptionCI{

	private BCMAsyncContentNodeCompositeEndPoint server_edp;
	private ConcreteAsyncBCMEndPoint<DHTServicesCI> client_edp;
	protected final ResultEndPoint caller;
	protected final ConcurrentHashMap<String, CompletableFuture<ContentDataI>> pendingResults = new ConcurrentHashMap<>();

    protected FacadeAsyncComponent(
    		String uri, ConcreteAsyncBCMEndPoint<DHTServicesCI> client_edp , 
    		BCMAsyncContentNodeCompositeEndPoint server_edp) throws Exception {

		super(1, 0);
		this.caller = new ResultEndPoint(this);

        this.client_edp = client_edp;
        this.server_edp = server_edp;
        client_edp.initialiseServerSide(this);
        this.traceMessage("FacadeComponent initialisé" );
    }

    @Override
    public void start() throws ComponentStartException {
    	try {
			server_edp.initialiseClientSide(this);
		} catch (ConnectionException e) {
			e.printStackTrace();
		}
        super.start();
        this.traceMessage("FacadeComponent démarré.");
    }

    @Override
    public void finalise() throws Exception {
    	server_edp.cleanUpClientSide();
        this.traceMessage("FacadeComponent se termine...");
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
		//TODO il faut que le endpoint mette correctement dans le future.
		pendingResults.put(computationURI, cfuture);
		server_edp.getContentAccessEndpoint().getClientSideReference().get(computationURI, key, caller);
		ContentDataI res = cfuture.get();
		server_edp.getContentAccessEndpoint().getClientSideReference().clearComputation(computationURI);
		return res;
	}

	public ContentDataI put(ContentKeyI key, ContentDataI value) throws Exception {
		String computationURI = URIGenerator.generateURI();
		ContentDataI res = server_edp.getContentAccessEndpoint().getClientSideReference().putSync(computationURI, key, value);
		server_edp.getContentAccessEndpoint().getClientSideReference().clearComputation(computationURI);
		return res;
	}

	public ContentDataI remove(ContentKeyI key) throws Exception {
		String computationURI = URIGenerator.generateURI();
		ContentDataI res = server_edp.getContentAccessEndpoint().getClientSideReference().removeSync(computationURI, key);
		server_edp.getContentAccessEndpoint().getClientSideReference().clearComputation(computationURI);
		return res;
	}

	public <R extends Serializable, A extends Serializable> A mapReduce(
			SelectorI selector, 
			ProcessorI<R> processor,
			ReductorI<A, R> reductor, 
			CombinatorI<A> combinator, 
			A initialAcc) throws Exception {
			
		if (selector == null || processor == null || reductor == null || combinator == null || initialAcc == null) 
			throw new IllegalArgumentException("Parametre(s) de mapReduce null "); 
		
		String computationURI = URIGenerator.generateURI("MAP_REDUCE");
		server_edp.getMapReduceEndpoint().getClientSideReference().mapSync(computationURI, selector, processor);
		A res = server_edp.getMapReduceEndpoint().getClientSideReference().reduceSync(computationURI, reductor, combinator, initialAcc);
		server_edp.getMapReduceEndpoint().getClientSideReference().clearMapReduceComputation(computationURI);		
		return res;
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
}