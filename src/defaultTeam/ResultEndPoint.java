package defaultTeam;

import java.io.Serializable;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.Future;

import defaultTeam.port.ResultReceptionConnector;
import defaultTeam.port.ResultReceptionInboundPort;
import defaultTeam.port.ResultReceptionOutboundPort;
import fr.sorbonne_u.components.AbstractComponent;
import fr.sorbonne_u.components.ComponentI;
import fr.sorbonne_u.components.endpoints.BCMEndPoint;
import fr.sorbonne_u.components.endpoints.EndPointI;
import fr.sorbonne_u.components.exceptions.ConnectionException;
import fr.sorbonne_u.components.ports.AbstractInboundPort;
import fr.sorbonne_u.cps.dht_mapreduce.interfaces.content.ContentDataI;
import fr.sorbonne_u.cps.dht_mapreduce.interfaces.content.ResultReceptionCI;
import fr.sorbonne_u.cps.dht_mapreduce.interfaces.content.ResultReceptionI;

import java.util.concurrent.CompletableFuture;

public class ResultEndPoint extends BCMEndPoint<ResultReceptionCI> {

    /**
	 * 
	 */
	private static final long serialVersionUID = 1L;
	ComponentI owner;

	public ResultEndPoint(ComponentI owner) {
        super(ResultReceptionCI.class, ResultReceptionCI.class);
        this.owner = owner;
    }

	@Override
	//TODO : inboundPortUri ? weird
	protected AbstractInboundPort makeInboundPort(AbstractComponent c, String inboundPortURI) throws Exception {
		ResultReceptionInboundPort inboundPort = new ResultReceptionInboundPort(serverSideOfferedInterface, c, inboundPortURI);
		inboundPort.publishPort();
		return inboundPort;
	}

	@Override
	protected ResultReceptionCI makeOutboundPort(AbstractComponent c, String inboundPortURI) throws Exception {
		ResultReceptionOutboundPort outboundPort = new ResultReceptionOutboundPort(clientSideInterface, owner);
		outboundPort.publishPort();
		ResultReceptionConnector connector = new ResultReceptionConnector();
		outboundPort.doConnection(inboundPortURI, connector);
		return outboundPort;
	}

    
}