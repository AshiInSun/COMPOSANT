package defaultTeam.clients;

import fr.sorbonne_u.components.AbstractComponent;
import fr.sorbonne_u.components.annotations.RequiredInterfaces;
import fr.sorbonne_u.components.exceptions.ComponentStartException;
import fr.sorbonne_u.components.exceptions.ConnectionException;
import fr.sorbonne_u.cps.dht_mapreduce.interfaces.content.ContentDataI;
import fr.sorbonne_u.cps.dht_mapreduce.interfaces.content.ContentKeyI;
import fr.sorbonne_u.cps.dht_mapreduce.interfaces.frontend.DHTServicesCI;
import fr.sorbonne_u.cps.dht_mapreduce.interfaces.mapreduce.CombinatorI;
import fr.sorbonne_u.cps.dht_mapreduce.interfaces.mapreduce.ProcessorI;
import fr.sorbonne_u.cps.dht_mapreduce.interfaces.mapreduce.ReductorI;
import fr.sorbonne_u.cps.dht_mapreduce.interfaces.mapreduce.SelectorI;

import java.util.concurrent.CountDownLatch;

import defaultTeam.ContentKey;
import defaultTeam.Personne;
import defaultTeam.endpoints.ConcreteBCMEndPoint;
import defaultTeam.endpoints.DHTServicesEndPoint;

@RequiredInterfaces(required = {DHTServicesCI.class})
public class Client extends AbstractComponent {
    private static final String NOM = "NOM";
    private static final String AGE = "AGE";

    private DHTServicesEndPoint dht_edp;

    protected Client(String uri, DHTServicesEndPoint dht_edp) throws Exception {
        super(1, 0);
        this.dht_edp = dht_edp;
    }

    @Override
    public synchronized void start() throws ComponentStartException {
        try {
            dht_edp.initialiseClientSide(this);
        } catch (ConnectionException e) {
            e.printStackTrace();
        }
        super.start();
    }

    @Override
	    public void execute() throws Exception {
	        this.traceMessage("Client démarre les tests asynchrones...\n");
	
	        // Création de Personnes et de leur clef associée
	        ContentKeyI k1 = new ContentKey("123");
	        ContentKeyI k2 = new ContentKey("nextnode");
	        ContentKeyI k3 = new ContentKey("789");
	
	        ContentDataI p1 = new Personne("Alpha", 10);
	        ContentDataI p2 = new Personne("Beta", 16);
	        ContentDataI p3 = new Personne("Delta", 35);
	
	        // Ajout des données de manière asynchrone avec un CountDownLatch pour synchronisation
	        this.traceMessage("Insertion des données (asynchrone)...\n");
	        dht_edp.getClientSideReference().put(k1, p1);
	        this.traceMessage("Donnée insérée pour k1\n");

	        dht_edp.getClientSideReference().put(k2, p2);
            this.traceMessage("Donnée insérée pour k2\n");
	
	        dht_edp.getClientSideReference().put(k3, p3);
            this.traceMessage("Donnée insérée pour k3\n");

        // Récupération des données de manière asynchrone
        this.traceMessage("Récupération des données (asynchrone)...\n");

        ContentDataI result = dht_edp.getClientSideReference().get(k1);
        if (result != null) 
            this.traceMessage("Donnée pour k1: " + result.getValue(NOM) + ", " + result.getValue(AGE) + "\n");
        else
            this.traceMessage("Donnée pour k1 non trouvée.\n");
        ContentDataI resultA = dht_edp.getClientSideReference().get(k2);
        if (result != null) 
            this.traceMessage("Donnée pour k2: " + resultA.getValue(NOM) + ", " + resultA.getValue(AGE) + "\n");
        else
            this.traceMessage("Donnée pour k2 non trouvée.\n");

        // Moyenne des âges avec mapReduce asynchrone
        this.traceMessage("\nUtilisation de mapReduce (asynchrone) pour calculer l'âge moyen\n");

        SelectorI selector = data -> true;
        ProcessorI<Integer> processor = data -> (data instanceof Personne) ? (Integer) data.getValue(AGE) : 0;
        ReductorI<int[], Integer> reductor = (acc, age) -> new int[]{acc[0] + age, acc[1] + 1};
        CombinatorI<int[]> combinator = (acc1, acc2) -> new int[]{acc1[0] + acc2[0], acc1[1] + acc2[1]};
        int[] initialAcc = new int[]{0, 0};
        int[] res = dht_edp.getClientSideReference().mapReduce(selector, processor, reductor, combinator, initialAcc);
        double ageMoyen = (res[1] == 0) ? 0 : (double) res[0] / res[1];
        this.traceMessage("L'âge moyen est: " + ageMoyen + "\n");
    }

    @Override
    public synchronized void finalise() throws Exception {
        dht_edp.cleanUpClientSide();
        super.finalise();
    }

    @Override
    public synchronized void shutdown() throws fr.sorbonne_u.components.exceptions.ComponentShutdownException {
        super.shutdown();
    }
}