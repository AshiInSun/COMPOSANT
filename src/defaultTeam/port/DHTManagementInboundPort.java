package defaultTeam.port;

import fr.sorbonne_u.components.ComponentI;
import fr.sorbonne_u.cps.dht_mapreduce.interfaces.mapreduce.MapReduceCI;
import fr.sorbonne_u.cps.dht_mapreduce.interfaces.mapreduce.MapReduceResultReceptionCI;
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
import defaultTeam.NodeAsyncComponent;
import defaultTeam.old.NodeComponent;
import defaultTeam.port.sync.DHTMapReduceInboundPort;

import java.io.Serializable;

public class DHTManagementInboundPort extends DHTMapReduceInboundPort implements DHTManagementCI {
    private static final long serialVersionUID = 1L;
    public static final String MANAGEMENT_HANDLER_URI = "mah";

    public DHTManagementInboundPort(String uri, ComponentI owner) throws Exception {
        super(uri, owner);
    }

	@Override
	public void initialiseContent(NodeContentI content) throws Exception {
		this.getOwner().runTask(MANAGEMENT_HANDLER_URI, o -> {
	        try {
	            ((NodeAsyncComponent) o).initialiseContent(content);
	        } catch (Exception e) {
	            e.printStackTrace();
	        }
		});
	}

	@Override
	public NodeStateI getCurrentState() throws Exception {
		return this.getOwner().handleRequest(MANAGEMENT_HANDLER_URI, o -> {
	        try {
	            return ((NodeAsyncComponent) o).getCurrentState();
	        } catch (Exception e) {
	            e.printStackTrace();
	        }
			return null;
		});
	}

	@Override
	public NodeContentI suppressNode() throws Exception {
		return this.getOwner().handleRequest(MANAGEMENT_HANDLER_URI, o -> {
	        try {
	            return ((NodeAsyncComponent) o).suppressNode();
	        } catch (Exception e) {
	            e.printStackTrace();
	        }
			return null;
		});
	}

	@Override
	public <CI extends ResultReceptionCI> void split(String computationURI, LoadPolicyI loadPolicy,
			EndPointI<CI> caller) throws Exception {
		this.getOwner().runTask(MANAGEMENT_HANDLER_URI, o -> {
	        try {
	            ((NodeAsyncComponent) o).split(computationURI, loadPolicy, caller);
	        } catch (Exception e) {

	    		System.out.println(e);
	            e.printStackTrace();
	        }
		});
	}

	@Override
	public <CI extends ResultReceptionCI> void merge(String computationURI, LoadPolicyI loadPolicy,
			EndPointI<CI> caller) throws Exception {
		this.getOwner().runTask(MANAGEMENT_HANDLER_URI, o -> {
	        try {
	            ((NodeAsyncComponent) o).merge(computationURI, loadPolicy, caller);
	        } catch (Exception e) {

	    		System.out.println(e);
	            e.printStackTrace();
	        }
		});
	}

	@Override
	public void computeChords(String computationURI, int numberOfChords) throws Exception {
		this.getOwner().runTask(MANAGEMENT_HANDLER_URI, o -> {
	        try {
	            ((NodeAsyncComponent) o).computeChords(computationURI, numberOfChords);
	        } catch (Exception e) {
	            e.printStackTrace();
	        }
		});
	}

	@Override
	public SerializablePair<ContentNodeCompositeEndPointI<ContentAccessCI, ParallelMapReduceCI, DHTManagementCI>, Integer> getChordInfo(
			int offset) throws Exception {
		return this.getOwner().handleRequest(MANAGEMENT_HANDLER_URI, o -> {
	        try {
	            return ((NodeAsyncComponent) o).getChordInfo(offset);
	        } catch (Exception e) {
	            e.printStackTrace();
	        }
			return null;
		});
	}

	
}
