package defaultTeam.endpoints;

import defaultTeam.port.DHTAsyncContentAccessConnector;
import defaultTeam.port.DHTAsyncContentAccessInboundPort;
import defaultTeam.port.DHTAsyncContentAccessOutboundPort;
import fr.sorbonne_u.components.AbstractComponent;
import fr.sorbonne_u.components.AbstractPort;
import fr.sorbonne_u.components.endpoints.BCMEndPoint;
import fr.sorbonne_u.components.ports.AbstractInboundPort;
import fr.sorbonne_u.components.pre.dcc.connectors.DynamicComponentCreationConnector;
import fr.sorbonne_u.components.pre.dcc.interfaces.DynamicComponentCreationCI;
import fr.sorbonne_u.components.pre.dcc.ports.DynamicComponentCreationInboundPort;
import fr.sorbonne_u.components.pre.dcc.ports.DynamicComponentCreationOutboundPort;
import fr.sorbonne_u.cps.dht_mapreduce.interfaces.content.ContentAccessCI;

public class BCMDynamicComponentCreationEndPoint extends BCMEndPoint<DynamicComponentCreationCI> {

	private static final long serialVersionUID = 1L;

	public BCMDynamicComponentCreationEndPoint() {
		super(DynamicComponentCreationCI.class, DynamicComponentCreationCI.class);
	}

	@Override
	protected AbstractInboundPort makeInboundPort(AbstractComponent c, String inboundPortURI) throws Exception {
		DynamicComponentCreationInboundPort inBoundPort = new DynamicComponentCreationInboundPort(inboundPortURI,c);
		inBoundPort.publishPort();
		return inBoundPort;
	}

	@Override
	protected DynamicComponentCreationCI makeOutboundPort(AbstractComponent c, String inboundPortURI) throws Exception {
		String outboundPortURI = AbstractPort.generatePortURI(ContentAccessCI.class);
		DynamicComponentCreationOutboundPort outboundPort = new DynamicComponentCreationOutboundPort(outboundPortURI, c);
        outboundPort.publishPort();
        DynamicComponentCreationConnector connector = new DynamicComponentCreationConnector();
        c.doPortConnection(outboundPortURI, inboundPortURI, connector);
        return outboundPort;
	}

}
