package defaultTeam.endpoints;


import defaultTeam.port.sync.DHTServiceConnector;
import defaultTeam.port.sync.DHTServiceInboundPort;
import defaultTeam.port.sync.DHTServiceOutboundPort;
import fr.sorbonne_u.components.AbstractComponent;
import fr.sorbonne_u.components.AbstractPort;
import fr.sorbonne_u.components.endpoints.BCMEndPoint;
import fr.sorbonne_u.components.interfaces.OfferedCI;
import fr.sorbonne_u.components.ports.AbstractInboundPort;
import fr.sorbonne_u.cps.dht_mapreduce.interfaces.frontend.DHTServicesCI;
import fr.sorbonne_u.cps.dht_mapreduce.interfaces.mapreduce.MapReduceCI;

public class DHTServicesEndPoint extends BCMEndPoint<DHTServicesCI> {

	public DHTServicesEndPoint(String uri) {
		super(DHTServicesCI.class, DHTServicesCI.class);
	}

	private static final long serialVersionUID = 1L;



	@Override
	protected AbstractInboundPort makeInboundPort(AbstractComponent c, String inboundPortURI) throws Exception {
		DHTServiceInboundPort inBoundPort = new DHTServiceInboundPort(inboundPortURI,c);
		inBoundPort.publishPort();
		return inBoundPort;
	}

	@Override
	protected DHTServicesCI makeOutboundPort(AbstractComponent c, String inboundPortURI) throws Exception {
		String outboundPortURI = AbstractPort.generatePortURI(MapReduceCI.class);
		DHTServiceOutboundPort outBoundPort = new DHTServiceOutboundPort(inboundPortURI, c);
		outBoundPort.publishPort();
		DHTServiceConnector connector = new DHTServiceConnector();
		c.doPortConnection(outboundPortURI, inboundPortURI, connector);
		return outBoundPort;
	}

}
