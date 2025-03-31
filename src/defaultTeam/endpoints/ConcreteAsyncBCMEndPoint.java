package defaultTeam.endpoints;
import defaultTeam.port.DHTAsyncContentAccessConnector;
import defaultTeam.port.DHTAsyncContentAccessInboundPort;
import defaultTeam.port.DHTAsyncContentAccessOutboundPort;
import defaultTeam.port.DHTAsyncMapReduceConnector;
import defaultTeam.port.DHTAsyncMapReduceInboundPort;
import defaultTeam.port.DHTAsyncMapReduceOutboundPort;
import fr.sorbonne_u.components.AbstractComponent;
import fr.sorbonne_u.components.AbstractPort;
import fr.sorbonne_u.components.ports.AbstractInboundPort;
import fr.sorbonne_u.components.endpoints.BCMEndPoint;
import fr.sorbonne_u.cps.dht_mapreduce.interfaces.content.ContentAccessCI;
import fr.sorbonne_u.cps.dht_mapreduce.interfaces.mapreduce.MapReduceCI;


public class ConcreteAsyncBCMEndPoint<CI extends fr.sorbonne_u.components.interfaces.RequiredCI>
    extends BCMEndPoint<CI> {
	
	private static final long serialVersionUID = 1L;

	public ConcreteAsyncBCMEndPoint(Class<CI> implementedInterface,
                               Class<? extends fr.sorbonne_u.components.interfaces.OfferedCI> serverSideOfferedInterface,
                               String inboundPortURI) {
        super(implementedInterface, serverSideOfferedInterface, inboundPortURI);
    }

    @Override
    protected AbstractInboundPort makeInboundPort(AbstractComponent c, String inboundPortURI) throws Exception {
		  if(this.getServerSideInterface().equals(ContentAccessCI.class)) {
        	DHTAsyncContentAccessInboundPort inboundPort = new DHTAsyncContentAccessInboundPort(inboundPortURI, c);
			inboundPort.publishPort();
			return inboundPort;
        	
        
        }else if(this.getServerSideInterface().equals(MapReduceCI.class)) {
        	DHTAsyncMapReduceInboundPort inboundPort = new DHTAsyncMapReduceInboundPort(inboundPortURI, c);
			inboundPort.publishPort();
			return inboundPort;
        	
        }else {
            throw new IllegalArgumentException("Interface serveur inconnue : " + this.getServerSideInterface());
        }
    }
    
    @SuppressWarnings("unchecked")
	@Override
	protected CI makeOutboundPort(AbstractComponent c, String inboundPortURI) throws Exception {
    	
		if(this.getClientSideInterface().equals(ContentAccessCI.class)) {
        	String outboundPortURI = AbstractPort.generatePortURI(ContentAccessCI.class);
            DHTAsyncContentAccessOutboundPort outboundPort = new DHTAsyncContentAccessOutboundPort(outboundPortURI, c);
            outboundPort.publishPort();
            DHTAsyncContentAccessConnector connector = new DHTAsyncContentAccessConnector();
            c.doPortConnection(outboundPortURI, inboundPortURI, connector);
            //outboundPort.doConnection(inboundPortURI, connector);
            return (CI) outboundPort;
            
        }else if(this.getClientSideInterface().equals(MapReduceCI.class)) {
        	String outboundPortURI = AbstractPort.generatePortURI(MapReduceCI.class);
            DHTAsyncMapReduceOutboundPort outboundPort = new DHTAsyncMapReduceOutboundPort(outboundPortURI, c);
            outboundPort.publishPort();
            DHTAsyncMapReduceConnector connector = new DHTAsyncMapReduceConnector();
            c.doPortConnection(outboundPortURI, inboundPortURI, connector);
            return (CI) outboundPort;
        }else {
            throw new IllegalArgumentException("Interface client inconnue : " + this.getClientSideInterface());
        }
	}
}