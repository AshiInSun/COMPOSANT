package defaultTeam;

import fr.sorbonne_u.components.cvm.AbstractCVM;

public class DHTCVM extends AbstractCVM {

    public DHTCVM() throws Exception {
		super();
	}

	protected FacadeComponent facade;
    protected NodeComponent node1, node2;
    protected Client client;

    @Override
    public void deploy() throws Exception {
    	// TODO : la gen des uri !! jsplus comment faire
        // Création de la façade
        this.facade = new FacadeComponent();
        this.addDeployedComponent("URIFC", this.facade);

        // Création des nœuds
        this.node1 = new NodeComponent(0, 99);
        this.addDeployedComponent("URINC1", this.node1);

        this.node2 = new NodeComponent(100, 199);
        this.addDeployedComponent("URINC2", this.node2);

        // Connexion des nœuds entre eux
        this.node1.connectToNextNode(
            node2.getContentAccessEndpointURI(),
            node2.getMapReduceEndpointURI(),
            node2.getServicesEndpointURI()
        );
        
        this.node2.connectToNextNode(
                node1.getContentAccessEndpointURI(),
                node1.getMapReduceEndpointURI(),
                node1.getServicesEndpointURI()
            );

        // Connexion de la façade au premier nœud
        this.facade.connectToDHT(
            node1.getContentAccessEndpointURI(),
            node1.getMapReduceEndpointURI(),
            node1.getServicesEndpointURI()
        );

        // Création du client
        this.client = new Client(this.facade);
        this.addDeployedComponent("URIC1", this.client);

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
