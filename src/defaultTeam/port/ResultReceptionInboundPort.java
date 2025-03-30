package defaultTeam.port;

import java.io.Serializable;

import defaultTeam.FacadeAsyncComponent;
import fr.sorbonne_u.components.ComponentI;
import fr.sorbonne_u.components.interfaces.OfferedCI;
import fr.sorbonne_u.components.ports.AbstractInboundPort;
import fr.sorbonne_u.cps.dht_mapreduce.interfaces.content.ContentDataI;
import fr.sorbonne_u.cps.dht_mapreduce.interfaces.content.ResultReceptionCI;

public class ResultReceptionInboundPort  extends AbstractInboundPort implements ResultReceptionCI{
	/**
	 * 
	 */
	private static final long serialVersionUID = 1L;

	public ResultReceptionInboundPort(Class<? extends OfferedCI> implementedInterface, ComponentI owner, String uri)
			throws Exception {
		super(uri, implementedInterface, owner);
	}

	@Override
	public void acceptResult(String computationURI, Serializable result) throws Exception {
		((FacadeAsyncComponent) this.owner).acceptResult(computationURI, result);
	}

}
