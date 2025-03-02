package defaultTeam;
import java.io.Serializable;

import defaultTeam.port.DHTContentAccessConnector;
import defaultTeam.port.DHTMapReduceConnector;
import defaultTeam.port.DHTServiceConnector;
import fr.sorbonne_u.components.AbstractComponent;
import fr.sorbonne_u.components.exceptions.ComponentShutdownException;
import fr.sorbonne_u.components.exceptions.ComponentStartException;
import fr.sorbonne_u.cps.dht_mapreduce.interfaces.content.ContentAccessSyncCI;
import fr.sorbonne_u.cps.dht_mapreduce.interfaces.content.ContentDataI;
import fr.sorbonne_u.cps.dht_mapreduce.interfaces.content.ContentKeyI;
import fr.sorbonne_u.cps.dht_mapreduce.interfaces.frontend.DHTServicesCI;
import fr.sorbonne_u.cps.dht_mapreduce.interfaces.mapreduce.CombinatorI;
import fr.sorbonne_u.cps.dht_mapreduce.interfaces.mapreduce.ProcessorI;
import fr.sorbonne_u.cps.dht_mapreduce.interfaces.mapreduce.ReductorI;
import fr.sorbonne_u.cps.dht_mapreduce.interfaces.mapreduce.SelectorI;


public class FacadeComponent extends AbstractComponent implements DHTServicesCI {

	private BCMContentNodeCompositeEndPoint compositeEndpoint;

    public FacadeComponent() throws Exception {
        super(1, 0);

        this.compositeEndpoint = new BCMContentNodeCompositeEndPoint();

        this.traceMessage("FacadeComponent initialisé" );
    }

    @Override
    public void start() throws ComponentStartException {
        super.start();
        this.traceMessage("FacadeComponent démarré.");
    }

    @Override
    public void execute() throws Exception {
        this.traceMessage("FacadeComponent exécute ses opérations...");
    }

    @Override
    public void finalise() throws Exception {
        this.traceMessage("FacadeComponent se termine...");
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
    public void connectToDHT(String firstNodeContentAccessURI, 
            String firstNodeMapReduceURI, 
            String firstNodeServicesURI) throws Exception {

			this.doPortConnection(
			compositeEndpoint.getContentAccessEndpoint().getOutboundPortURI(),
			firstNodeContentAccessURI,
			DHTContentAccessConnector.class.getCanonicalName());

			this.doPortConnection(
			compositeEndpoint.getMapReduceEndpoint().getOutboundPortURI(),
			firstNodeMapReduceURI,
			DHTMapReduceConnector.class.getCanonicalName());
			
			this.doPortConnection(
			compositeEndpoint.getServicesEndpoint().getOutboundPortURI(),
			firstNodeServicesURI,
			DHTServiceConnector.class.getCanonicalName());
			
			this.traceMessage("FacadeComponent connecté au premier nœud du DHT.");
	}

	@Override
	public ContentDataI get(ContentKeyI key) throws Exception {
		return ((ContentAccessSyncCI) compositeEndpoint.getContentAccessEndpoint()).getSync("FacadeComputation", key);
	}

	@Override
	public ContentDataI put(ContentKeyI key, ContentDataI value) throws Exception {
		return ((ContentAccessSyncCI) compositeEndpoint.getContentAccessEndpoint()).putSync("FacadeComputation", key, value);
	}

	@Override
	public ContentDataI remove(ContentKeyI key) throws Exception {
		return ((ContentAccessSyncCI) compositeEndpoint.getContentAccessEndpoint()).removeSync("FacadeComputation", key);
	}

	@Override
	public <R extends Serializable, A extends Serializable> A mapReduce(SelectorI selector, ProcessorI<R> processor,
			ReductorI<A, R> reductor, CombinatorI<A> combinator, A initialAcc) throws Exception {
		
		return ((DHTServicesCI) compositeEndpoint.getMapReduceEndpoint()).mapReduce(selector, processor, reductor, combinator, initialAcc);
	}
}