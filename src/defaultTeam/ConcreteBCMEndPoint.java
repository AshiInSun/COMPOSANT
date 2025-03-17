package defaultTeam;
import defaultTeam.port.DHTContentAccessConnector;
import defaultTeam.port.DHTContentAccessInboundPort;
import defaultTeam.port.DHTContentAccessOutboundPort;
import defaultTeam.port.DHTMapReduceConnector;
import defaultTeam.port.DHTMapReduceInboundPort;
import defaultTeam.port.DHTMapReduceOutboundPort;
import defaultTeam.port.DHTServiceConnector;
import defaultTeam.port.DHTServiceInboundPort;
import defaultTeam.port.DHTServiceOutboundPort;
import fr.sorbonne_u.components.AbstractComponent;
import fr.sorbonne_u.components.ports.AbstractInboundPort;
import fr.sorbonne_u.components.endpoints.BCMEndPoint;
import fr.sorbonne_u.cps.dht_mapreduce.interfaces.content.ContentAccessSyncCI;
import fr.sorbonne_u.cps.dht_mapreduce.interfaces.frontend.DHTServicesCI;
import fr.sorbonne_u.cps.dht_mapreduce.interfaces.mapreduce.MapReduceSyncCI;


public class ConcreteBCMEndPoint<CI extends fr.sorbonne_u.components.interfaces.RequiredCI>
    extends BCMEndPoint<CI> {
	
	private static final long serialVersionUID = 1L;

	public ConcreteBCMEndPoint(Class<CI> implementedInterface,
                               Class<? extends fr.sorbonne_u.components.interfaces.OfferedCI> serverSideOfferedInterface,
                               String inboundPortURI) {
        super(implementedInterface, serverSideOfferedInterface, inboundPortURI);
    }

    @Override
    protected AbstractInboundPort makeInboundPort(AbstractComponent c, String inboundPortURI) throws Exception {
		 if(this.getServerSideInterface().equals(DHTServicesCI.class)) {
			 DHTServiceInboundPort inboundPort = new DHTServiceInboundPort(inboundPortURI, c);
			 inboundPort.publishPort();
			 return inboundPort;
            
        } else if(this.getServerSideInterface().equals(ContentAccessSyncCI.class)){
	    	DHTContentAccessInboundPort inboundPort = new DHTContentAccessInboundPort(inboundPortURI, c);
			inboundPort.publishPort();
			return inboundPort;
        	
        }else if(this.getServerSideInterface().equals(MapReduceSyncCI.class)) {
        	DHTMapReduceInboundPort inboundPort = new DHTMapReduceInboundPort(inboundPortURI, c);
			inboundPort.publishPort();
			return inboundPort;
        	
        }else {
            throw new IllegalArgumentException("Interface serveur inconnue : " + this.getServerSideInterface());
        }
    }
    
    @SuppressWarnings("unchecked")
	@Override
	protected CI makeOutboundPort(AbstractComponent c, String inboundPortURI) throws Exception {
    	
		if (this.getClientSideInterface().equals(DHTServicesCI.class)) {
            DHTServiceOutboundPort outboundPort = new DHTServiceOutboundPort(c);
            outboundPort.publishPort();
            DHTServiceConnector connector = new DHTServiceConnector();
            outboundPort.doConnection(inboundPortURI, connector);
            return  (CI) outboundPort;
            
		}else if(this.getClientSideInterface().equals(ContentAccessSyncCI.class)) {
            DHTContentAccessOutboundPort outboundPort = new DHTContentAccessOutboundPort(c);
            outboundPort.publishPort();
            DHTContentAccessConnector connector = new DHTContentAccessConnector();
            outboundPort.doConnection(inboundPortURI, connector);
            return (CI) outboundPort;
            
        }else if(this.getClientSideInterface().equals(MapReduceSyncCI.class)) {
            DHTMapReduceOutboundPort outboundPort = new DHTMapReduceOutboundPort(c);
            outboundPort.publishPort();
            DHTMapReduceConnector connector = new DHTMapReduceConnector();
            outboundPort.doConnection(inboundPortURI, connector);
            return (CI) outboundPort;
            
        }else {
            throw new IllegalArgumentException("Interface client inconnue : " + this.getClientSideInterface());
        }
	}
}