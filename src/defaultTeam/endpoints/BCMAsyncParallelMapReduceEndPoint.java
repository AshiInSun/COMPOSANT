package defaultTeam.endpoints;

import defaultTeam.port.DHTAsyncParallelMapReduceConnector;
import defaultTeam.port.DHTAsyncParallelMapReduceInboundPort;
import defaultTeam.port.DHTAsyncParallelMapReduceOutboundPort;
import fr.sorbonne_u.components.AbstractComponent;
import fr.sorbonne_u.components.AbstractPort;
import fr.sorbonne_u.components.endpoints.BCMEndPoint;
import fr.sorbonne_u.components.ports.AbstractInboundPort;
import fr.sorbonne_u.cps.dht_mapreduce.interfaces.mapreduce.MapReduceCI;
import fr.sorbonne_u.cps.dht_mapreduce.interfaces.mapreduce.ParallelMapReduceCI;


public class BCMAsyncParallelMapReduceEndPoint extends BCMEndPoint<ParallelMapReduceCI> {

	private static final long serialVersionUID = 1L;

	public BCMAsyncParallelMapReduceEndPoint() {
		super(ParallelMapReduceCI.class,ParallelMapReduceCI.class);
	}

	@Override
	protected AbstractInboundPort makeInboundPort(AbstractComponent c, String inboundPortURI) throws Exception {
		DHTAsyncParallelMapReduceInboundPort inBoundPort = new DHTAsyncParallelMapReduceInboundPort(inboundPortURI,c);
		inBoundPort.publishPort();
		return inBoundPort;
	}

	@Override
	protected ParallelMapReduceCI makeOutboundPort(AbstractComponent c, String inboundPortURI) throws Exception {
		String outboundPortURI = AbstractPort.generatePortURI(ParallelMapReduceCI.class);
        DHTAsyncParallelMapReduceOutboundPort outboundPort = new DHTAsyncParallelMapReduceOutboundPort(outboundPortURI, c);
        outboundPort.publishPort();
        DHTAsyncParallelMapReduceConnector connector = new DHTAsyncParallelMapReduceConnector();
        c.doPortConnection(outboundPortURI, inboundPortURI, connector);
        return outboundPort;
	}

}
