package defaultTeam.old;
import defaultTeam.endpoints.BCMAsyncContentAccessEndPoint;
import defaultTeam.endpoints.BCMAsyncMapReduceEndPoint;
import fr.sorbonne_u.components.endpoints.BCMCompositeEndPoint;
import fr.sorbonne_u.cps.dht_mapreduce.interfaces.endpoints.ContentNodeBaseCompositeEndPointI;
import fr.sorbonne_u.cps.dht_mapreduce.interfaces.endpoints.ContentNodeCompositeEndPointI;
import fr.sorbonne_u.cps.dht_mapreduce.interfaces.management.DHTManagementCI;
import fr.sorbonne_u.cps.dht_mapreduce.interfaces.content.ContentAccessCI;
import fr.sorbonne_u.cps.dht_mapreduce.interfaces.mapreduce.MapReduceCI;
import fr.sorbonne_u.cps.dht_mapreduce.interfaces.mapreduce.ParallelMapReduceCI;


public class BCMAsyncContentNodeCompositeEndPointOld extends BCMCompositeEndPoint 
    	implements ContentNodeBaseCompositeEndPointI<ContentAccessCI, MapReduceCI> {

    private static final long serialVersionUID = 1L;
	protected static final int NUMBER_OF_ENDPOINTS = 2;

    public BCMAsyncContentNodeCompositeEndPointOld() {
        super(NUMBER_OF_ENDPOINTS);
 
        BCMAsyncContentAccessEndPoint contentAccessEndpoint =
        		new BCMAsyncContentAccessEndPoint();
        this.addEndPoint(contentAccessEndpoint);
        
        BCMAsyncMapReduceEndPoint mapReduceEndpoint =
        		new BCMAsyncMapReduceEndPoint();
        this.addEndPoint(mapReduceEndpoint);
    }

    @Override
    public BCMAsyncContentAccessEndPoint getContentAccessEndpoint() {
        return (BCMAsyncContentAccessEndPoint) this.getEndPoint(ContentAccessCI.class);
    }

    @Override
    public BCMAsyncMapReduceEndPoint getMapReduceEndpoint() {
        return (BCMAsyncMapReduceEndPoint) this.getEndPoint(MapReduceCI.class);
    }
}
