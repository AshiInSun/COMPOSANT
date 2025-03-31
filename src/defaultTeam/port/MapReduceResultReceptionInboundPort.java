package defaultTeam.port;

import java.io.Serializable;


import defaultTeam.FacadeAsyncComponent;
import fr.sorbonne_u.components.ComponentI;
import fr.sorbonne_u.components.interfaces.OfferedCI;
import fr.sorbonne_u.components.ports.AbstractInboundPort;
import fr.sorbonne_u.cps.dht_mapreduce.interfaces.content.ContentDataI;
import fr.sorbonne_u.cps.dht_mapreduce.interfaces.content.ResultReceptionCI;
import fr.sorbonne_u.cps.dht_mapreduce.interfaces.mapreduce.MapReduceResultReceptionCI;

public class MapReduceResultReceptionInboundPort  extends AbstractInboundPort implements MapReduceResultReceptionCI{
	/**
	 * 
	 */
	private static final long serialVersionUID = 1L;

	public MapReduceResultReceptionInboundPort(Class<? extends OfferedCI> implementedInterface, ComponentI owner, String uri)
			throws Exception {
		super(uri, implementedInterface, owner);
	}

	@Override
	public void acceptResult(String computationURI, String emitterId, Serializable acc) throws Exception {
		((FacadeAsyncComponent) this.owner).acceptResult(computationURI, emitterId, acc);
	}
}
