package defaultTeam.port;


import fr.sorbonne_u.cps.dht_mapreduce.interfaces.mapreduce.MapReduceCI;
import fr.sorbonne_u.cps.dht_mapreduce.interfaces.mapreduce.MapReduceResultReceptionCI;
import fr.sorbonne_u.cps.dht_mapreduce.interfaces.mapreduce.MapReduceSyncCI;
import fr.sorbonne_u.cps.dht_mapreduce.interfaces.mapreduce.ParallelMapReduceCI;
import fr.sorbonne_u.components.endpoints.EndPointI;
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

import defaultTeam.port.sync.DHTMapReduceConnector;

public class DHTManagementConnector extends DHTMapReduceConnector implements DHTManagementCI {

	@Override
	public void initialiseContent(NodeContentI content) throws Exception {
		((DHTManagementCI) this.offering).initialiseContent(content);
	}

	@Override
	public NodeStateI getCurrentState() throws Exception {
		return ((DHTManagementCI) this.offering).getCurrentState();
	}

	@Override
	public NodeContentI suppressNode() throws Exception {
		return ((DHTManagementCI) this.offering).suppressNode();
	}

	@Override
	public <CI extends ResultReceptionCI> void split(String computationURI, LoadPolicyI loadPolicy,
			EndPointI<CI> caller) throws Exception {
		((DHTManagementCI) this.offering).split(computationURI, loadPolicy, caller);
	}

	@Override
	public <CI extends ResultReceptionCI> void merge(String computationURI, LoadPolicyI loadPolicy,
			EndPointI<CI> caller) throws Exception {
		((DHTManagementCI) this.offering).merge(computationURI, loadPolicy, caller);
	}

	@Override
	public void computeChords(String computationURI, int numberOfChords) throws Exception {
		((DHTManagementCI) this.offering).computeChords(computationURI, numberOfChords);
	}

	@Override
	public SerializablePair<ContentNodeCompositeEndPointI<ContentAccessCI, ParallelMapReduceCI, DHTManagementCI>, Integer> getChordInfo(
			int offset) throws Exception {
		return ((DHTManagementCI) this.offering).getChordInfo(offset);
	}


	
}