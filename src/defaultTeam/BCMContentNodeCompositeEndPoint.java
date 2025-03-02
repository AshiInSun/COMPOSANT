package defaultTeam;
import fr.sorbonne_u.components.endpoints.CompositeEndPoint;
import fr.sorbonne_u.components.ports.PortI;
import fr.sorbonne_u.components.AbstractPort;
import fr.sorbonne_u.components.endpoints.BCMEndPoint;
import fr.sorbonne_u.cps.dht_mapreduce.interfaces.endpoints.ContentNodeBaseCompositeEndPointI;
import fr.sorbonne_u.cps.dht_mapreduce.interfaces.frontend.DHTServicesCI;
import fr.sorbonne_u.cps.dht_mapreduce.interfaces.content.ContentAccessSyncCI;
import fr.sorbonne_u.cps.dht_mapreduce.interfaces.mapreduce.MapReduceSyncCI;
import fr.sorbonne_u.exceptions.PostconditionException;

public class BCMContentNodeCompositeEndPoint extends CompositeEndPoint 
    	implements ContentNodeBaseCompositeEndPointI<ContentAccessSyncCI, MapReduceSyncCI> {

    protected static final int NUMBER_OF_ENDPOINTS = 3;

    public BCMContentNodeCompositeEndPoint() {
        super(NUMBER_OF_ENDPOINTS);
 
        BCMEndPoint<ContentAccessSyncCI> contentAccessEndpoint =
        		new ConcreteBCMEndPoint<ContentAccessSyncCI>(
                	ContentAccessSyncCI.class,
                	ContentAccessSyncCI.class,
                	AbstractPort.generatePortURI(ContentAccessSyncCI.class));
        this.addEndPoint(contentAccessEndpoint);
        
        BCMEndPoint<MapReduceSyncCI> mapReduceEndpoint =
        		new ConcreteBCMEndPoint<MapReduceSyncCI>(
                    MapReduceSyncCI.class,
                    MapReduceSyncCI.class,
                    AbstractPort.generatePortURI(MapReduceSyncCI.class));
        this.addEndPoint(mapReduceEndpoint);
        
        BCMEndPoint<DHTServicesCI> servicesEndpoint =
                new ConcreteBCMEndPoint<>(
                        DHTServicesCI.class,
                        DHTServicesCI.class,
                        AbstractPort.generatePortURI(DHTServicesCI.class));
        this.addEndPoint(servicesEndpoint);
        
        assert complete() : new PostconditionException("complete()");
    }

    @Override
    public BCMEndPoint<ContentAccessSyncCI> getContentAccessEndpoint() {
        return (BCMEndPoint<ContentAccessSyncCI>) this.getEndPoint(ContentAccessSyncCI.class);
    }

    @Override
    public BCMEndPoint<MapReduceSyncCI> getMapReduceEndpoint() {
        return (BCMEndPoint<MapReduceSyncCI>) this.getEndPoint(MapReduceSyncCI.class);
    }
    
    public BCMEndPoint<DHTServicesCI> getServicesEndpoint() {
        return (BCMEndPoint<DHTServicesCI>) this.getEndPoint(DHTServicesCI.class);
    }

    @SuppressWarnings("unchecked")
	@Override
    public ContentNodeBaseCompositeEndPointI<ContentAccessSyncCI, MapReduceSyncCI> copyWithSharable() {
        return (ContentNodeBaseCompositeEndPointI<ContentAccessSyncCI, MapReduceSyncCI>) super.copyWithSharable();
    }

    public void unpublishEndPoints() throws Exception {
        ((PortI) this.getContentAccessEndpoint()).unpublishPort();
        ((PortI) this.getMapReduceEndpoint()).unpublishPort();
        ((PortI) this.getServicesEndpoint()).unpublishPort();
    }
}
