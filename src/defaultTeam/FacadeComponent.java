package defaultTeam;
import java.io.Serializable;

import fr.sorbonne_u.components.AbstractComponent;
import fr.sorbonne_u.components.exceptions.ComponentShutdownException;
import fr.sorbonne_u.components.exceptions.ComponentStartException;
import fr.sorbonne_u.cps.dht_mapreduce.interfaces.content.ContentDataI;
import fr.sorbonne_u.cps.dht_mapreduce.interfaces.content.ContentKeyI;
import fr.sorbonne_u.cps.dht_mapreduce.interfaces.frontend.DHTServicesCI;
import fr.sorbonne_u.cps.dht_mapreduce.interfaces.mapreduce.CombinatorI;
import fr.sorbonne_u.cps.dht_mapreduce.interfaces.mapreduce.ProcessorI;
import fr.sorbonne_u.cps.dht_mapreduce.interfaces.mapreduce.ReductorI;
import fr.sorbonne_u.cps.dht_mapreduce.interfaces.mapreduce.SelectorI;
import fr.sorbonne_u.cps.mapreduce.utils.URIGenerator;


public class FacadeComponent extends AbstractComponent implements DHTServicesCI {

	private BCMContentNodeCompositeEndPoint server_edp;
	private ConcreteBCMEndPoint<DHTServicesCI> client_edp;

    public FacadeComponent(String uri, ConcreteBCMEndPoint<DHTServicesCI> client_edp , BCMContentNodeCompositeEndPoint server_edp) throws Exception {
        super(1, 0);

        this.client_edp = client_edp;
        this.server_edp = server_edp;
        client_edp.initialiseServerSide(this);
        this.traceMessage("FacadeComponent initialisé" );
    }

    @Override
    public void start() throws ComponentStartException {
    	server_edp.initialiseClientSide(this);
        super.start();
        this.traceMessage("FacadeComponent démarré.");
    }

    @Override
    public void execute() throws Exception {
        this.traceMessage("FacadeComponent exécute ses opérations...");
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

	@Override
	public ContentDataI get(ContentKeyI key) throws Exception {
		String computationURI = URIGenerator.generateURI();
		ContentDataI res = server_edp.getContentAccessEndpoint().getClientSideReference().getSync(computationURI, key);
		server_edp.getContentAccessEndpoint().getClientSideReference().clearComputation(computationURI);
		return res;
	}

	@Override
	public ContentDataI put(ContentKeyI key, ContentDataI value) throws Exception {
		String computationURI = URIGenerator.generateURI();
		ContentDataI res = server_edp.getContentAccessEndpoint().getClientSideReference().putSync(computationURI, key, value);
		server_edp.getContentAccessEndpoint().getClientSideReference().clearComputation(computationURI);
		return res;
	}

	@Override
	public ContentDataI remove(ContentKeyI key) throws Exception {
		String computationURI = URIGenerator.generateURI();
		ContentDataI res = server_edp.getContentAccessEndpoint().getClientSideReference().removeSync(computationURI, key);
		server_edp.getContentAccessEndpoint().getClientSideReference().clearComputation(computationURI);
		return res;
	}

	@Override
	public <R extends Serializable, A extends Serializable> A mapReduce(
			SelectorI selector, 
			ProcessorI<R> processor,
			ReductorI<A, R> reductor, 
			CombinatorI<A> combinator, 
			A initialAcc) throws Exception {
		
			String computationURI = URIGenerator.generateURI();
			server_edp.getMapReduceEndpoint().getClientSideReference().mapSync(computationURI, selector, processor);
			A res = server_edp.getMapReduceEndpoint().getClientSideReference().reduceSync(computationURI, reductor, combinator, initialAcc);
			server_edp.getMapReduceEndpoint().getClientSideReference().clearMapReduceComputation(computationURI);
			return res;
	}
}