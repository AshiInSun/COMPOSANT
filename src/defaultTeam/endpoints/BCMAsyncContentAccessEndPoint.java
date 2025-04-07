package defaultTeam.endpoints;

import defaultTeam.port.DHTAsyncContentAccessConnector;
import defaultTeam.port.DHTAsyncContentAccessInboundPort;
import defaultTeam.port.DHTAsyncContentAccessOutboundPort;
import fr.sorbonne_u.components.AbstractComponent;
import fr.sorbonne_u.components.AbstractPort;
import fr.sorbonne_u.components.endpoints.BCMEndPoint;
import fr.sorbonne_u.components.ports.AbstractInboundPort;
import fr.sorbonne_u.cps.dht_mapreduce.interfaces.content.ContentAccessCI;

public class BCMAsyncContentAccessEndPoint extends BCMEndPoint<ContentAccessCI> {

	private static final long serialVersionUID = 1L;

	public BCMAsyncContentAccessEndPoint() {
		super(ContentAccessCI.class,ContentAccessCI.class);
	}

	@Override
	protected AbstractInboundPort makeInboundPort(AbstractComponent c, String inboundPortURI) throws Exception {
		DHTAsyncContentAccessInboundPort inBoundPort = new DHTAsyncContentAccessInboundPort(inboundPortURI,c);
		inBoundPort.publishPort();
		return inBoundPort;
	}

	@Override
	protected ContentAccessCI makeOutboundPort(AbstractComponent c, String inboundPortURI) throws Exception {
		String outboundPortURI = AbstractPort.generatePortURI(ContentAccessCI.class);
        DHTAsyncContentAccessOutboundPort outboundPort = new DHTAsyncContentAccessOutboundPort(outboundPortURI, c);
        outboundPort.publishPort();
        DHTAsyncContentAccessConnector connector = new DHTAsyncContentAccessConnector();
        c.doPortConnection(outboundPortURI, inboundPortURI, connector);
        return outboundPort;
	}

}
