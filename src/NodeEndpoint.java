import fr.sorbonne_u.cps.dht_mapreduce.interfaces.endpoints.ContentNodeBaseCompositeEndPointI;
import fr.sorbonne_u.components.endpoints.EndPointI;
import fr.sorbonne_u.cps.dht_mapreduce.interfaces.content.ContentAccessSyncI;
import fr.sorbonne_u.cps.dht_mapreduce.interfaces.mapreduce.MapReduceSyncI;
import fr.sorbonne_u.cps.mapreduce.endpoints.POJOContentNodeCompositeEndPoint;

public class NodeEndpoint extends POJOContentNodeCompositeEndPoint {
	
	private final Node node;
    private final EndPointI<ContentAccessSyncI> contentAccessEndpoint;
    private final EndPointI<MapReduceSyncI> mapReduceEndpoint;
    
    public NodeEndpoint(Node node) {
    	this.node = node;
        this.contentAccessEndpoint = new NodeContentAccessEndpoint(node);
        this.mapReduceEndpoint = new NodeMapReduceEndpoint(node);
    }

	@Override
	public EndPointI<ContentAccessSyncI> getContentAccessEndpoint() {
		return this.contentAccessEndpoint;
	}

	@Override
	public EndPointI<MapReduceSyncI> getMapReduceEndpoint() {
		return this.mapReduceEndpoint;
	}
	
	@Override
	public ContentNodeBaseCompositeEndPointI<ContentAccessSyncI, MapReduceSyncI> copyWithSharable() {
		// TODO Auto-generated method stub
		return null;
	}

}
