package defaultTeam.port;

import java.io.Serializable;

import fr.sorbonne_u.components.ComponentI;
import fr.sorbonne_u.components.interfaces.RequiredCI;
import fr.sorbonne_u.components.ports.AbstractOutboundPort;
import fr.sorbonne_u.cps.dht_mapreduce.interfaces.content.ContentAccessSyncCI;
import fr.sorbonne_u.cps.dht_mapreduce.interfaces.content.ResultReceptionCI;
import fr.sorbonne_u.cps.dht_mapreduce.interfaces.frontend.DHTServicesCI;

public class ResultReceptionOutboundPort  extends AbstractOutboundPort implements ResultReceptionCI{

	/**
	 * 
	 */
	private static final long serialVersionUID = 1L;

	public ResultReceptionOutboundPort(Class<? extends RequiredCI> implementedInterface, ComponentI owner, String uri)
			throws Exception {
		super(uri, implementedInterface, owner);
	}
	public ResultReceptionOutboundPort(Class<? extends RequiredCI> implementedInterface, ComponentI owner)
			throws Exception {
		super(implementedInterface, owner);
	}

	@Override
	public void acceptResult(String computationURI, Serializable result) throws Exception {
		((ResultReceptionCI) this.getConnector()).acceptResult(computationURI, result);
	}

}
