package defaultTeam;

import defaultTeam.endpoints.BCMAsyncContentNodeCompositeEndPoint;
import fr.sorbonne_u.components.AbstractComponent;
import fr.sorbonne_u.cps.dht_mapreduce.interfaces.content.ContentAccessCI;
import fr.sorbonne_u.cps.dht_mapreduce.interfaces.content.ContentAccessSyncCI;
import fr.sorbonne_u.cps.dht_mapreduce.interfaces.content.ResultReceptionCI;
import fr.sorbonne_u.cps.dht_mapreduce.interfaces.frontend.DHTServicesCI;
import fr.sorbonne_u.cps.dht_mapreduce.interfaces.management.DHTManagementCI;
import fr.sorbonne_u.cps.dht_mapreduce.interfaces.mapreduce.MapReduceCI;
import fr.sorbonne_u.cps.dht_mapreduce.interfaces.mapreduce.MapReduceResultReceptionCI;
import fr.sorbonne_u.cps.dht_mapreduce.interfaces.mapreduce.MapReduceSyncCI;
import fr.sorbonne_u.cps.mapreduce.utils.URIGenerator;
import fr.sorbonne_u.components.annotations.OfferedInterfaces;
import fr.sorbonne_u.components.annotations.RequiredInterfaces;

@OfferedInterfaces(offered = {ContentAccessSyncCI.class, MapReduceSyncCI.class, 
        ContentAccessCI.class, MapReduceCI.class, DHTServicesCI.class, 
        ResultReceptionCI.class, MapReduceResultReceptionCI.class,
        DHTManagementCI.class})
@RequiredInterfaces(required = {ContentAccessSyncCI.class, MapReduceSyncCI.class, 
        ContentAccessCI.class, MapReduceCI.class, 
        ResultReceptionCI.class, MapReduceResultReceptionCI.class,
        DHTManagementCI.class})
public class ChordInitializer extends AbstractComponent {

    protected final int numberOfChords;
    protected final BCMAsyncContentNodeCompositeEndPoint managementEndpoint;

    protected ChordInitializer(int numberOfChords, BCMAsyncContentNodeCompositeEndPoint endpoint) throws Exception {
        super(1, 0);
        this.numberOfChords = numberOfChords;
        this.managementEndpoint = endpoint;
    }

    @Override
    public void execute() throws Exception {
        this.traceMessage(">>> Initialisation des cordes...\n");
        managementEndpoint.initialiseClientSide(this);
        this.traceMessage("→ Appel de computeChords sur \n");
        String computationURI = URIGenerator.generateURI();
    	managementEndpoint.getDHTManagementEndpoint().getClientSideReference().computeChords(computationURI, numberOfChords);
        managementEndpoint.cleanUpClientSide();
        

        this.traceMessage("✅ Toutes les cordes sont initialisées.\n");
    }

    @Override
    public void finalise() throws Exception {
        super.finalise();
    }
}
