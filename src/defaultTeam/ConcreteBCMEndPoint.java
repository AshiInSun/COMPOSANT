package defaultTeam;
import defaultTeam.port.DHTServiceConnector;
import defaultTeam.port.DHTServiceInboundPort;
import defaultTeam.port.DHTServiceOutboundPort;
import fr.sorbonne_u.components.AbstractComponent;
import fr.sorbonne_u.components.AbstractPort;
import fr.sorbonne_u.components.ports.AbstractInboundPort;
import fr.sorbonne_u.components.endpoints.BCMEndPoint;
import fr.sorbonne_u.cps.dht_mapreduce.interfaces.frontend.DHTServicesCI;


public class ConcreteBCMEndPoint<CI extends fr.sorbonne_u.components.interfaces.RequiredCI>
    extends BCMEndPoint<CI> {
	
	private static final long serialVersionUID = 1L;

	public ConcreteBCMEndPoint(Class<CI> implementedInterface,
                               Class<? extends fr.sorbonne_u.components.interfaces.OfferedCI> serverSideOfferedInterface,
                               String inboundPortURI) {
        super(implementedInterface, serverSideOfferedInterface, inboundPortURI, null);
    }

    @Override
    protected AbstractInboundPort makeInboundPort(AbstractComponent c, String inboundPortURI) throws Exception {
        if (this.getServerSideInterface().equals(DHTServicesCI.class)) {
            return new DHTServiceInboundPort(inboundPortURI, c);
        } else {
            throw new IllegalArgumentException("Interface serveur inconnue : " + this.getServerSideInterface());
        }
    }
    
    @SuppressWarnings("unchecked")
	@Override
	protected CI makeOutboundPort(AbstractComponent c, String outboundPortURI, String inboundPortURI) throws Exception {
		if (this.getClientSideInterface().equals(DHTServicesCI.class)) {
        	String outboundPortURI1 = AbstractPort.generatePortURI(DHTServicesCI.class);
            DHTServiceOutboundPort outboundPort = new DHTServiceOutboundPort(outboundPortURI1, c);
            outboundPort.publishPort();
            c.doPortConnection(outboundPort.getPortURI(), inboundPortURI, DHTServiceConnector.class.getCanonicalName());
            return (CI) outboundPort;
        } else {
            throw new IllegalArgumentException("Interface client inconnue : " + this.getClientSideInterface());
        }
	}
}