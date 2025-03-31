package defaultTeam.port;

import java.io.Serializable;

import fr.sorbonne_u.components.ComponentI;
import fr.sorbonne_u.components.interfaces.RequiredCI;
import fr.sorbonne_u.components.ports.AbstractOutboundPort;
import fr.sorbonne_u.cps.dht_mapreduce.interfaces.content.ContentAccessSyncCI;
import fr.sorbonne_u.cps.dht_mapreduce.interfaces.content.ResultReceptionCI;
import fr.sorbonne_u.cps.dht_mapreduce.interfaces.frontend.DHTServicesCI;
import fr.sorbonne_u.cps.dht_mapreduce.interfaces.mapreduce.MapReduceResultReceptionCI;

public class MapReduceResultReceptionOutboundPort  extends AbstractOutboundPort implements MapReduceResultReceptionCI{

	/**
	 * 
	 */
	private static final long serialVersionUID = 1L;

	public MapReduceResultReceptionOutboundPort(Class<? extends RequiredCI> implementedInterface, ComponentI owner, String uri)
			throws Exception {
		super(uri, implementedInterface, owner);
	}
	public MapReduceResultReceptionOutboundPort(Class<? extends RequiredCI> implementedInterface, ComponentI owner)
			throws Exception {
		super(implementedInterface, owner);
	}
	@Override
	public void acceptResult(String computationURI, String emitterId, Serializable acc) throws Exception {
		((MapReduceResultReceptionCI) this.getConnector()).acceptResult(computationURI, emitterId, acc);
	}
}
