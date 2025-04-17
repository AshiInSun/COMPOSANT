package defaultTeam;

import defaultTeam.endpoints.BCMAsyncContentNodeCompositeEndPoint;
import defaultTeam.endpoints.DHTServicesEndPoint;
import fr.sorbonne_u.components.AbstractComponent;
import fr.sorbonne_u.components.AbstractPort;
import fr.sorbonne_u.components.cvm.AbstractCVM;
import fr.sorbonne_u.cps.dht_mapreduce.interfaces.frontend.DHTServicesCI;
import fr.sorbonne_u.exceptions.VerboseException;

public class DHTCVM extends AbstractCVM {

    private static final int NB_NODES = 8;
    private static final int SIZE_NODES = 30;
    private static final int NB_CLIENTS = 4;
    private static final int NB_CLIENTS_MR = 5;
 // Flag de test
    boolean flag = false;
    boolean flagInsert = false;
    boolean flagCA = true;
    boolean flagMR = false;

    public DHTCVM() throws Exception {
        super();
    }

    @Override
    public void deploy() throws Exception {

        String uri_facade = "f1";

        // Point de connexion partagé pour la façade et les nœuds
        BCMAsyncContentNodeCompositeEndPoint dht_node = new BCMAsyncContentNodeCompositeEndPoint();
        BCMAsyncContentNodeCompositeEndPoint chord_edp = new BCMAsyncContentNodeCompositeEndPoint();
        DHTServicesEndPoint dht_client =
          		 new DHTServicesEndPoint(uri_facade);
        // Création des nœuds
        BCMAsyncContentNodeCompositeEndPoint[] endPointsNode =
                new BCMAsyncContentNodeCompositeEndPoint[NB_NODES];

        String[] urinode = new String[NB_NODES];

        for (int i = 0; i < NB_NODES; i++) {
            endPointsNode[i] = new BCMAsyncContentNodeCompositeEndPoint();
        }
        
     // Création de la façade
        String uri_facade_component = AbstractComponent.createComponent(
                FacadeAsyncComponent.class.getCanonicalName(),
                new Object[]{
                        uri_facade,
                        dht_client.copyWithSharable(),  // endpoint côté client de la façade
                        dht_node.copyWithSharable(),
                        NB_NODES
                });
        
        for (int i = 0; i < NB_NODES; i++) {
            String uri = AbstractComponent.createComponent(
                    NodeAsyncComponent.class.getCanonicalName(),
                    new Object[]{
                            "node" + i,
                            i * SIZE_NODES,
                            ((i + 1) * SIZE_NODES) - 1,
                            dht_node.copyWithSharable(),
                            endPointsNode[i].copyWithSharable(),
                            endPointsNode[(i + 1) % NB_NODES].copyWithSharable(),
                            NB_NODES
                    });
            urinode[i] = uri;
            this.toggleTracing(uri);
        }
        
        this.toggleTracing(uri_facade_component);

        if(flag || flagInsert) {
        	// Création de plusieurs clients (NB_CLIENTS)
            for (int i = 0; i < NB_CLIENTS; i++) {
                String uri_client = AbstractPort.generatePortURI(DHTServicesCI.class);

                String uri_client_component = AbstractComponent.createComponent(
                        ClientInserteur.class.getCanonicalName(),
                        new Object[]{
                                uri_client,
                                dht_client.copyWithSharable()
                        });

                this.toggleTracing(uri_client_component);
            }
        }
        if(flag || flagCA) {
        	//Client pour test de base
            String uri_client = AbstractPort.generatePortURI(DHTServicesCI.class);

            String uri_client_component = AbstractComponent.createComponent(
                    ClientCAtest.class.getCanonicalName(),
                    new Object[]{
                            uri_client,
                            dht_client.copyWithSharable()
                    });

            this.toggleTracing(uri_client_component);
        }
        if(flag || flagMR) {
        	for (int i = 0; i < NB_CLIENTS_MR; i++) {
        	    String clientUri = "MRClient" + i;
        	    String uriMR = AbstractComponent.createComponent(
        	        ClientMRtest.class.getCanonicalName(),
        	        new Object[]{clientUri, dht_client.copyWithSharable()}
        	    );
        	    this.toggleTracing(uriMR);
        	}
        }
        	
        super.deploy();
    }

    public static void main(String[] args) {
        VerboseException.VERBOSE = true;
        try {
            DHTCVM cvm = new DHTCVM();
            cvm.startStandardLifeCycle(1000000);
            System.exit(0);
        } catch (Exception e) {
            e.printStackTrace();
        }
    }
}
