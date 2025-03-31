package defaultTeam;

import fr.sorbonne_u.cps.dht_mapreduce.interfaces.mapreduce.MapReduceResultReceptionCI;
import defaultTeam.port.MapReduceResultReceptionConnector;
import defaultTeam.port.MapReduceResultReceptionInboundPort;
import defaultTeam.port.MapReduceResultReceptionOutboundPort;
import fr.sorbonne_u.components.AbstractComponent;
import fr.sorbonne_u.components.endpoints.BCMEndPoint;
import fr.sorbonne_u.components.ports.AbstractInboundPort;

public class MapReduceResultEndPoint extends BCMEndPoint<MapReduceResultReceptionCI>{

    /**
	 * 
	 */
	private static final long serialVersionUID = 1L;

	public MapReduceResultEndPoint(String uri) {
		super(MapReduceResultReceptionCI.class, MapReduceResultReceptionCI.class, uri);
    }

	@Override
	protected AbstractInboundPort makeInboundPort(AbstractComponent c, String inboundPortURI) throws Exception {
		MapReduceResultReceptionInboundPort inboundPort = new MapReduceResultReceptionInboundPort(serverSideOfferedInterface, c, inboundPortURI);
		inboundPort.publishPort();
		return inboundPort;
	}

	@Override
	protected MapReduceResultReceptionCI makeOutboundPort(AbstractComponent c, String inboundPortURI) throws Exception {
		MapReduceResultReceptionOutboundPort outboundPort = new MapReduceResultReceptionOutboundPort(clientSideInterface, c);
		outboundPort.publishPort();
		MapReduceResultReceptionConnector connector = new MapReduceResultReceptionConnector();
		outboundPort.doConnection(inboundPortURI, connector);
		return (MapReduceResultReceptionCI) outboundPort;
	}
}