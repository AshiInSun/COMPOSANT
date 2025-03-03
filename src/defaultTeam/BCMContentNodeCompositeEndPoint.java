package defaultTeam;
import fr.sorbonne_u.components.AbstractPort;
import fr.sorbonne_u.components.endpoints.BCMCompositeEndPoint;
import fr.sorbonne_u.cps.dht_mapreduce.interfaces.endpoints.ContentNodeBaseCompositeEndPointI;
import fr.sorbonne_u.cps.dht_mapreduce.interfaces.content.ContentAccessSyncCI;
import fr.sorbonne_u.cps.dht_mapreduce.interfaces.mapreduce.MapReduceSyncCI;

public class BCMContentNodeCompositeEndPoint extends BCMCompositeEndPoint 
    	implements ContentNodeBaseCompositeEndPointI<ContentAccessSyncCI, MapReduceSyncCI> {

    private static final long serialVersionUID = 1L;
	protected static final int NUMBER_OF_ENDPOINTS = 2;

    public BCMContentNodeCompositeEndPoint() {
        super(NUMBER_OF_ENDPOINTS);
 
        ConcreteBCMEndPoint<ContentAccessSyncCI> contentAccessEndpoint =
        		new ConcreteBCMEndPoint<ContentAccessSyncCI>(
                	ContentAccessSyncCI.class,
                	ContentAccessSyncCI.class,
                	AbstractPort.generatePortURI(ContentAccessSyncCI.class));
        this.addEndPoint(contentAccessEndpoint);
        
        ConcreteBCMEndPoint<MapReduceSyncCI> mapReduceEndpoint =
        		new ConcreteBCMEndPoint<MapReduceSyncCI>(
                    MapReduceSyncCI.class,
                    MapReduceSyncCI.class,
                    AbstractPort.generatePortURI(MapReduceSyncCI.class));
        this.addEndPoint(mapReduceEndpoint);
    }

    @Override
    public ConcreteBCMEndPoint<ContentAccessSyncCI> getContentAccessEndpoint() {
        return (ConcreteBCMEndPoint<ContentAccessSyncCI>) this.getEndPoint(ContentAccessSyncCI.class);
    }

    @Override
    public ConcreteBCMEndPoint<MapReduceSyncCI> getMapReduceEndpoint() {
        return (ConcreteBCMEndPoint<MapReduceSyncCI>) this.getEndPoint(MapReduceSyncCI.class);
    }
}
