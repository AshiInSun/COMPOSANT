package defaultTeam.endpoints;
import fr.sorbonne_u.components.AbstractPort;
import fr.sorbonne_u.components.endpoints.BCMCompositeEndPoint;
import fr.sorbonne_u.cps.dht_mapreduce.interfaces.endpoints.ContentNodeBaseCompositeEndPointI;
import fr.sorbonne_u.cps.dht_mapreduce.interfaces.content.ContentAccessCI;
import fr.sorbonne_u.cps.dht_mapreduce.interfaces.content.ContentAccessSyncCI;
import fr.sorbonne_u.cps.dht_mapreduce.interfaces.mapreduce.MapReduceCI;
import fr.sorbonne_u.cps.dht_mapreduce.interfaces.mapreduce.MapReduceSyncCI;

public class BCMAsyncContentNodeCompositeEndPoint extends BCMCompositeEndPoint 
    	implements ContentNodeBaseCompositeEndPointI<ContentAccessCI, MapReduceCI> {

    private static final long serialVersionUID = 1L;
	protected static final int NUMBER_OF_ENDPOINTS = 2;

    public BCMAsyncContentNodeCompositeEndPoint() {
        super(NUMBER_OF_ENDPOINTS);
 
        ConcreteAsyncBCMEndPoint<ContentAccessCI> contentAccessEndpoint =
        		new ConcreteAsyncBCMEndPoint<ContentAccessCI>(
                	ContentAccessCI.class,
                	ContentAccessCI.class,
                	AbstractPort.generatePortURI(ContentAccessCI.class));
        this.addEndPoint(contentAccessEndpoint);
        
        ConcreteAsyncBCMEndPoint<MapReduceCI> mapReduceEndpoint =
        		new ConcreteAsyncBCMEndPoint<MapReduceCI>(
                    MapReduceCI.class,
                    MapReduceCI.class,
                    AbstractPort.generatePortURI(MapReduceCI.class));
        this.addEndPoint(mapReduceEndpoint);
    }

    @Override
    public ConcreteAsyncBCMEndPoint<ContentAccessCI> getContentAccessEndpoint() {
        return (ConcreteAsyncBCMEndPoint<ContentAccessCI>) this.getEndPoint(ContentAccessCI.class);
    }

    @Override
    public ConcreteAsyncBCMEndPoint<MapReduceCI> getMapReduceEndpoint() {
        return (ConcreteAsyncBCMEndPoint<MapReduceCI>) this.getEndPoint(MapReduceCI.class);
    }
}
