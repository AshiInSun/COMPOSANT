package defaultTeam.endpoints;

import defaultTeam.port.DHTAsyncMapReduceConnector;
import defaultTeam.port.DHTAsyncMapReduceInboundPort;
import defaultTeam.port.DHTAsyncMapReduceOutboundPort;
import fr.sorbonne_u.components.AbstractComponent;
import fr.sorbonne_u.components.AbstractPort;
import fr.sorbonne_u.components.endpoints.BCMEndPoint;
import fr.sorbonne_u.components.ports.AbstractInboundPort;
import fr.sorbonne_u.cps.dht_mapreduce.interfaces.mapreduce.MapReduceCI;


public class BCMAsyncMapReduceEndPoint extends BCMEndPoint<MapReduceCI> {

	private static final long serialVersionUID = 1L;

	public BCMAsyncMapReduceEndPoint() {
		super(MapReduceCI.class,MapReduceCI.class);
	}

	@Override
	protected AbstractInboundPort makeInboundPort(AbstractComponent c, String inboundPortURI) throws Exception {
		DHTAsyncMapReduceInboundPort inBoundPort = new DHTAsyncMapReduceInboundPort(inboundPortURI,c);
		inBoundPort.publishPort();
		return inBoundPort;
	}

	@Override
	protected MapReduceCI makeOutboundPort(AbstractComponent c, String inboundPortURI) throws Exception {
		String outboundPortURI = AbstractPort.generatePortURI(MapReduceCI.class);
        DHTAsyncMapReduceOutboundPort outboundPort = new DHTAsyncMapReduceOutboundPort(outboundPortURI, c);
        outboundPort.publishPort();
        DHTAsyncMapReduceConnector connector = new DHTAsyncMapReduceConnector();
        c.doPortConnection(outboundPortURI, inboundPortURI, connector);
        return outboundPort;
	}

}
