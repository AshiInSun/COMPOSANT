package defaultTeam;
import fr.sorbonne_u.components.AbstractComponent;
import fr.sorbonne_u.components.exceptions.ComponentStartException;
import fr.sorbonne_u.cps.dht_mapreduce.interfaces.content.ContentDataI;
import fr.sorbonne_u.cps.dht_mapreduce.interfaces.content.ContentKeyI;
import fr.sorbonne_u.cps.dht_mapreduce.interfaces.frontend.DHTServicesCI;
import fr.sorbonne_u.cps.dht_mapreduce.interfaces.mapreduce.CombinatorI;
import fr.sorbonne_u.cps.dht_mapreduce.interfaces.mapreduce.ProcessorI;
import fr.sorbonne_u.cps.dht_mapreduce.interfaces.mapreduce.ReductorI;
import fr.sorbonne_u.cps.dht_mapreduce.interfaces.mapreduce.SelectorI;

public class Client extends AbstractComponent{
	private final static String NOM = "NOM";
	private static final String AGE = "AGE";
	
	ConcreteBCMEndPoint<DHTServicesCI> dht_edp;
	
	protected Client(String uri,ConcreteBCMEndPoint<DHTServicesCI> dht_edp) throws Exception {
        super(1, 0);
        this.dht_edp = dht_edp;
    }
	
	@Override
	public synchronized void start() throws ComponentStartException{
		dht_edp.initialiseClientSide(this);
		super.start();
	}
	
	@Override
    public void execute() throws Exception {
        this.traceMessage("Client démarre les tests...");

        // Création de Personnes et de leur clef associée
        ContentKeyI k1 = new ContentKey("123");
        ContentKeyI k2 = new ContentKey("nextnode");
        ContentKeyI k3 = new ContentKey("789");
        //ContentKeyI k4 = new ContentKey("123");

        ContentDataI p1 = new Personne("Alpha", 10);
        ContentDataI p2 = new Personne("Beta", 16);
        ContentDataI p3 = new Personne("Delta", 35);
        //ContentDataI p4 = new Personne("Omega", 82);

        // Ajout des données dans la table
        dht_edp.getClientSideReference().put(k1, p1);
        dht_edp.getClientSideReference().put(k2, p2);
        dht_edp.getClientSideReference().put(k3, p3);

        // Récupération des données
        this.traceMessage("Récupération des données...");
        ContentDataI result1 = dht_edp.getClientSideReference().get(k1);
        this.traceMessage("Donnée pour k1: " + result1.getValue(NOM) + ", " + result1.getValue(AGE));

        // Moyenne des âges avec mapReduce
        this.traceMessage("\nUtilisation de mapReduce pour calculer l'âge moyen");
        SelectorI selector = data -> true;
        ProcessorI<Integer> processor = data -> (data instanceof Personne) ? (Integer) data.getValue(AGE) : 0;
        ReductorI<int[], Integer> reductor = (acc, age) -> new int[]{acc[0] + age, acc[1] + 1};
        CombinatorI<int[]> combinator = (acc1, acc2) -> new int[]{acc1[0] + acc2[0], acc1[1] + acc2[1]};
        int[] initialAcc = new int[]{0, 0};

        int[] res = dht_edp.getClientSideReference().mapReduce(selector, processor, reductor, combinator, initialAcc);
        double ageMoyen = (res[1] == 0) ? 0 : (double) res[0] / res[1];
        this.traceMessage("L'âge moyen est: " + ageMoyen);
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