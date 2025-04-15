package defaultTeam;

import defaultTeam.endpoints.BCMAsyncContentNodeCompositeEndPoint;
import defaultTeam.endpoints.DHTServicesEndPoint;
import fr.sorbonne_u.components.AbstractComponent;
import fr.sorbonne_u.components.AbstractPort;
import fr.sorbonne_u.components.cvm.AbstractCVM;
import fr.sorbonne_u.cps.dht_mapreduce.interfaces.frontend.DHTServicesCI;
import fr.sorbonne_u.exceptions.VerboseException;

public class DHTCVM extends AbstractCVM {

    private static final int NB_NODES = 2;
    private static final int SIZE_NODES = 100;
    private static final int NB_CLIENTS = 3;

    public DHTCVM() throws Exception {
        super();
    }

    @Override
    public void deploy() throws Exception {

        String uri_facade = "f1";

        // Point de connexion partagé pour la façade et les nœuds
        BCMAsyncContentNodeCompositeEndPoint dht_node = new BCMAsyncContentNodeCompositeEndPoint();
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
                        dht_node.copyWithSharable()
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
                            endPointsNode[(i + 1) % NB_NODES].copyWithSharable()
                    });
            urinode[i] = uri;
            this.toggleTracing(uri);
        }
        
        this.toggleTracing(uri_facade_component);

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

        // Tracer les nœuds pour voir les opérations
        for (String uri : urinode) {
            this.toggleTracing(uri);
        }

        super.deploy();
    }

    public static void main(String[] args) {
        VerboseException.VERBOSE = true;
        try {
            DHTCVM cvm = new DHTCVM();
            cvm.startStandardLifeCycle(10000000);
            System.exit(0);
        } catch (Exception e) {
            e.printStackTrace();
        }
    }
}
