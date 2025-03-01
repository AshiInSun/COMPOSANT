package defaultTeam;
import java.io.Serializable;

import defaultTeam.port.DHTServiceInboundPort;
import defaultTeam.port.DHTServiceOutboundPort;
import fr.sorbonne_u.components.AbstractComponent;
import fr.sorbonne_u.components.exceptions.ComponentShutdownException;
import fr.sorbonne_u.components.exceptions.ComponentStartException;
import fr.sorbonne_u.components.exceptions.ConnectionException;
import fr.sorbonne_u.cps.dht_mapreduce.interfaces.content.ContentDataI;
import fr.sorbonne_u.cps.dht_mapreduce.interfaces.content.ContentKeyI;
import fr.sorbonne_u.cps.dht_mapreduce.interfaces.frontend.DHTServicesCI;
import fr.sorbonne_u.cps.dht_mapreduce.interfaces.frontend.DHTServicesI;
import fr.sorbonne_u.cps.dht_mapreduce.interfaces.mapreduce.CombinatorI;
import fr.sorbonne_u.cps.dht_mapreduce.interfaces.mapreduce.ProcessorI;
import fr.sorbonne_u.cps.dht_mapreduce.interfaces.mapreduce.ReductorI;
import fr.sorbonne_u.cps.dht_mapreduce.interfaces.mapreduce.SelectorI;
import fr.sorbonne_u.cps.mapreduce.endpoints.POJOContentNodeCompositeEndPoint;
import fr.sorbonne_u.cps.mapreduce.utils.URIGenerator;


public class FacadeComponent extends AbstractComponent implements DHTServicesCI {

    private final DHTServiceInboundPort inboundPort;
    private final DHTServiceOutboundPort outboundPort;

    public FacadeComponent(String inboundURI, String outboundURI) throws Exception {
        super(1, 0);

        this.inboundPort = new DHTServiceInboundPort(inboundURI, this);
        this.outboundPort = new DHTServiceOutboundPort(outboundURI, this);

        this.inboundPort.publishPort();
        this.outboundPort.publishPort();

        this.traceMessage("FacadeComponent initialisé avec les ports : " + inboundURI + " / " + outboundURI);
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
        this.doPortDisconnection(this.outboundPort.getPortURI());
        super.finalise();
    }

    @Override
    public void shutdown() {
        try {
			this.inboundPort.unpublishPort();
		} catch (Exception e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		}
        try {
			this.outboundPort.unpublishPort();
		} catch (Exception e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		}
        try {
			super.shutdown();
		} catch (ComponentShutdownException e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		}
    }

    public String getInboundPortURI() throws Exception {
        return this.inboundPort.getPortURI();
    }

    public String getOutboundPortURI() throws Exception {
        return this.outboundPort.getPortURI();
    }

	@Override
	public ContentDataI get(ContentKeyI key) throws Exception {
		return this.outboundPort.get(key);
	}

	@Override
	public ContentDataI put(ContentKeyI key, ContentDataI value) throws Exception {
		return this.outboundPort.put(key, value);
	}

	@Override
	public ContentDataI remove(ContentKeyI key) throws Exception {
		return this.outboundPort.remove(key);
	}

	@Override
	public <R extends Serializable, A extends Serializable> A mapReduce(SelectorI selector, ProcessorI<R> processor,
			ReductorI<A, R> reductor, CombinatorI<A> combinator, A initialAcc) throws Exception {
		
		// La methode n'est pas encore implementer dans DHTConnector, il faut la faire
		return this.outboundPort.mapReduce(selector, processor, reductor, combinator, initialAcc);
	}
}