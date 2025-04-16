package defaultTeam.endpoints;
import fr.sorbonne_u.components.endpoints.BCMCompositeEndPoint;
import fr.sorbonne_u.components.endpoints.EndPointI;
import fr.sorbonne_u.cps.dht_mapreduce.interfaces.endpoints.ContentNodeBaseCompositeEndPointI;
import fr.sorbonne_u.cps.dht_mapreduce.interfaces.endpoints.ContentNodeCompositeEndPointI;
import fr.sorbonne_u.cps.dht_mapreduce.interfaces.management.DHTManagementCI;
import fr.sorbonne_u.cps.dht_mapreduce.interfaces.content.ContentAccessCI;
import fr.sorbonne_u.cps.dht_mapreduce.interfaces.mapreduce.MapReduceCI;
import fr.sorbonne_u.cps.dht_mapreduce.interfaces.mapreduce.ParallelMapReduceCI;


public class BCMAsyncContentNodeCompositeEndPoint extends BCMCompositeEndPoint 
    	implements ContentNodeCompositeEndPointI<ContentAccessCI, ParallelMapReduceCI, DHTManagementCI> {

    private static final long serialVersionUID = 1L;
	protected static final int NUMBER_OF_ENDPOINTS = 3;

    public BCMAsyncContentNodeCompositeEndPoint() {
        super(NUMBER_OF_ENDPOINTS);
 
        BCMAsyncContentAccessEndPoint contentAccessEndpoint =
        		new BCMAsyncContentAccessEndPoint();
        this.addEndPoint(contentAccessEndpoint);
        
        BCMAsyncParallelMapReduceEndPoint mapReduceEndpoint =
        		new BCMAsyncParallelMapReduceEndPoint();
        this.addEndPoint(mapReduceEndpoint);
        
        BCMAsyncDHTManagementEndPoint DHTManagementEndpoint =
        		new BCMAsyncDHTManagementEndPoint();
        this.addEndPoint(DHTManagementEndpoint);
    }

    @Override
    public BCMAsyncContentAccessEndPoint getContentAccessEndpoint() {
        return (BCMAsyncContentAccessEndPoint) this.getEndPoint(ContentAccessCI.class);
    }

    @Override
    public EndPointI<ParallelMapReduceCI> getMapReduceEndpoint() {
        return (EndPointI<ParallelMapReduceCI>) this.getEndPoint(ParallelMapReduceCI.class);
    }

	@Override
	public EndPointI<DHTManagementCI> getDHTManagementEndpoint() {
		return (BCMAsyncDHTManagementEndPoint) this.getEndPoint(DHTManagementCI.class);
	}
}
