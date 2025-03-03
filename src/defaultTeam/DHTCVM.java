package defaultTeam;

import fr.sorbonne_u.components.AbstractComponent;
import fr.sorbonne_u.components.cvm.AbstractCVM;
import fr.sorbonne_u.cps.dht_mapreduce.interfaces.frontend.DHTServicesCI;

public class DHTCVM extends AbstractCVM {
	
	private static final int NB_NODES = 2; 
    public DHTCVM() throws Exception {
		super();
	}

    @Override
    public void deploy() throws Exception {
    	
    	String uri_facade = "f1";
    	String uri_client = "c1";
   
        BCMContentNodeCompositeEndPoint dht_node =
        		new BCMContentNodeCompositeEndPoint();

        @SuppressWarnings({ "unchecked", "rawtypes" })
		ConcreteBCMEndPoint<DHTServicesCI> dht_client =
        		 new ConcreteBCMEndPoint(DHTServicesCI.class, DHTServicesCI.class, uri_client);
        
        
        BCMContentNodeCompositeEndPoint[] endPointsNode = new BCMContentNodeCompositeEndPoint[NB_NODES];
        
        endPointsNode[0] = new BCMContentNodeCompositeEndPoint();
        endPointsNode[1] = new BCMContentNodeCompositeEndPoint();
        
        for(int i=0; i<NB_NODES; i++) {
        	String uri =  AbstractComponent.createComponent(
					NodeComponent.class.getCanonicalName(),
					new Object[] {
						"node"+i,
						i*100, // index
						((i+1)*100)-1, 
						(i+1)*100, 
						dht_node.copyWithSharable(),
						endPointsNode [i].copyWithSharable(),
						endPointsNode [(i+1)%NB_NODES].copyWithSharable()
					});
        }
        
      //Composant Facade
	String uri3 = AbstractComponent.createComponent(
			FacadeComponent.class.getCanonicalName(),
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
