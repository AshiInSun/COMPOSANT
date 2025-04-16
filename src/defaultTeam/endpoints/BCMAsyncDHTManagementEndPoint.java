package defaultTeam.endpoints;

import defaultTeam.port.DHTAsyncParallelMapReduceConnector;
import defaultTeam.port.DHTAsyncParallelMapReduceInboundPort;
import defaultTeam.port.DHTAsyncParallelMapReduceOutboundPort;
import defaultTeam.port.DHTManagementConnector;
import defaultTeam.port.DHTManagementInboundPort;
import defaultTeam.port.DHTManagementOutboundPort;
import fr.sorbonne_u.components.AbstractComponent;
import fr.sorbonne_u.components.AbstractPort;
import fr.sorbonne_u.components.endpoints.BCMEndPoint;
import fr.sorbonne_u.components.ports.AbstractInboundPort;
import fr.sorbonne_u.cps.dht_mapreduce.interfaces.management.DHTManagementCI;
import fr.sorbonne_u.cps.dht_mapreduce.interfaces.mapreduce.MapReduceCI;
import fr.sorbonne_u.cps.dht_mapreduce.interfaces.mapreduce.ParallelMapReduceCI;


public class BCMAsyncDHTManagementEndPoint extends BCMEndPoint<DHTManagementCI> {

	private static final long serialVersionUID = 1L;

	public BCMAsyncDHTManagementEndPoint() {
		super(DHTManagementCI.class,DHTManagementCI.class);
	}

	@Override
	protected AbstractInboundPort makeInboundPort(AbstractComponent c, String inboundPortURI) throws Exception {
		DHTManagementInboundPort inBoundPort = new DHTManagementInboundPort(inboundPortURI,c);
		inBoundPort.publishPort();
		return inBoundPort;
	}

	@Override
	protected DHTManagementCI makeOutboundPort(AbstractComponent c, String inboundPortURI) throws Exception {
		String outboundPortURI = AbstractPort.generatePortURI(DHTManagementCI.class);
        DHTManagementOutboundPort outboundPort = new DHTManagementOutboundPort(outboundPortURI, c);
        outboundPort.publishPort();
        DHTManagementConnector connector = new DHTManagementConnector();
        c.doPortConnection(outboundPortURI, inboundPortURI, connector);
        return outboundPort;
	}

}
