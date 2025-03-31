package defaultTeam;

import fr.sorbonne_u.components.AbstractComponent;
import fr.sorbonne_u.components.cvm.AbstractCVM;
import fr.sorbonne_u.cps.dht_mapreduce.interfaces.frontend.DHTServicesCI;

public class DHTCVM extends AbstractCVM {
	
	private static final int NB_NODES = 2;
	private static final int SIZE_NODES = 100;
    public DHTCVM() throws Exception {
		super();
	}

    @Override
    public void deploy() throws Exception {
    	
    	String uri_facade = "f1";
    	String uri_client = "c1";
   
        BCMAsyncContentNodeCompositeEndPoint dht_node =
        		new BCMAsyncContentNodeCompositeEndPoint();

        @SuppressWarnings({ "unchecked", "rawtypes" })
		ConcreteBCMEndPoint<DHTServicesCI> dht_client =
        		 new ConcreteBCMEndPoint(DHTServicesCI.class, DHTServicesCI.class, uri_client);
        
        
        BCMAsyncContentNodeCompositeEndPoint[] endPointsNode = new BCMAsyncContentNodeCompositeEndPoint[NB_NODES];
        
        endPointsNode[0] = new BCMAsyncContentNodeCompositeEndPoint();
        endPointsNode[1] = new BCMAsyncContentNodeCompositeEndPoint();
        String[] urinode = new String[NB_NODES];


        
        for(int i=0; i<NB_NODES; i++) {
        	String uri =  AbstractComponent.createComponent(
					AsyncNodeComponent.class.getCanonicalName(),
					new Object[] {
						"node"+i,
						i*SIZE_NODES, // index
						((i+1)*SIZE_NODES)-1, 
						dht_node.copyWithSharable(),
						endPointsNode [i].copyWithSharable(),
						endPointsNode [(i+1)%NB_NODES].copyWithSharable()
					});
        	urinode[i] = uri;
        }
        
        //Composant Facade
        String uri3 = AbstractComponent.createComponent(
			FacadeAsyncComponent.class.getCanonicalName(),
			new Object[] {
					uri_facade,
					dht_client.copyWithSharable(),
					dht_node.copyWithSharable()
			});
	
        String uri4 = AbstractComponent.createComponent(
			Client.class.getCanonicalName(),
			new Object[] {
					uri_client,
					dht_client.copyWithSharable()
			});
	
	this.toggleTracing(uri4);
	for(int i=0; i<NB_NODES; i++) {
		this.toggleTracing(urinode[i]);
	}
    super.deploy();
    }

    public static void main(String[] args) {
        try {
            DHTCVM cvm = new DHTCVM();
            cvm.startStandardLifeCycle(10000); // Exécution du système
            System.exit(0);
        } catch (Exception e) {
            e.printStackTrace();
        }
    }
}
