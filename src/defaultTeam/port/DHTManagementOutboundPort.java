package defaultTeam.port;


import fr.sorbonne_u.components.ComponentI;
import fr.sorbonne_u.components.endpoints.EndPointI;
import fr.sorbonne_u.cps.dht_mapreduce.interfaces.mapreduce.MapReduceCI;
import fr.sorbonne_u.cps.dht_mapreduce.interfaces.mapreduce.MapReduceResultReceptionCI;
import fr.sorbonne_u.cps.dht_mapreduce.interfaces.mapreduce.MapReduceSyncCI;
import fr.sorbonne_u.cps.dht_mapreduce.interfaces.mapreduce.ParallelMapReduceCI;
import fr.sorbonne_u.cps.dht_mapreduce.interfaces.content.ContentAccessCI;
import fr.sorbonne_u.cps.dht_mapreduce.interfaces.content.ResultReceptionCI;
import fr.sorbonne_u.cps.dht_mapreduce.interfaces.endpoints.ContentNodeCompositeEndPointI;
import fr.sorbonne_u.cps.dht_mapreduce.interfaces.management.DHTManagementCI;
import fr.sorbonne_u.cps.dht_mapreduce.interfaces.management.LoadPolicyI;
import fr.sorbonne_u.cps.dht_mapreduce.interfaces.mapreduce.CombinatorI;
import fr.sorbonne_u.cps.dht_mapreduce.interfaces.mapreduce.ProcessorI;
import fr.sorbonne_u.cps.dht_mapreduce.interfaces.mapreduce.ReductorI;
import fr.sorbonne_u.cps.dht_mapreduce.interfaces.mapreduce.SelectorI;
import fr.sorbonne_u.cps.mapreduce.utils.SerializablePair;

import java.io.Serializable;

import defaultTeam.port.sync.DHTMapReduceOutboundPort;

public class DHTManagementOutboundPort extends DHTMapReduceOutboundPort implements DHTManagementCI {
    private static final long serialVersionUID = 1L;
    
    public DHTManagementOutboundPort(String uri, ComponentI owner) throws Exception {
        super(uri, owner);
    }

	@Override
	public void initialiseContent(NodeContentI content) throws Exception {
		((DHTManagementCI) this.getConnector()).initialiseContent(content);
	}

	@Override
	public NodeStateI getCurrentState() throws Exception {
		return ((DHTManagementCI) this.getConnector()).getCurrentState();
	}

	@Override
	public NodeContentI suppressNode() throws Exception {
		return ((DHTManagementCI) this.getConnector()).suppressNode();
	}

	@Override
	public <CI extends ResultReceptionCI> void split(String computationURI, LoadPolicyI loadPolicy,
			EndPointI<CI> caller) throws Exception {
		((DHTManagementCI) this.getConnector()).split(computationURI, loadPolicy, caller);
	}

	@Override
	public <CI extends ResultReceptionCI> void merge(String computationURI, LoadPolicyI loadPolicy,
			EndPointI<CI> caller) throws Exception {
		((DHTManagementCI) this.getConnector()).merge(computationURI, loadPolicy, caller);
	}

	@Override
	public void computeChords(String computationURI, int numberOfChords) throws Exception {
		((DHTManagementCI) this.getConnector()).computeChords(computationURI, numberOfChords);
	}

	@Override
	public SerializablePair<ContentNodeCompositeEndPointI<ContentAccessCI, ParallelMapReduceCI, DHTManagementCI>, Integer> getChordInfo(
			int offset) throws Exception {
		return ((DHTManagementCI) this.getConnector()).getChordInfo(offset);
	}
    
    
}
