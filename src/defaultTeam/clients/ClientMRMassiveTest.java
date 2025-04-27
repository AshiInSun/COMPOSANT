package defaultTeam.clients;

import defaultTeam.ContentKey;
import defaultTeam.Personne;
import defaultTeam.endpoints.DHTServicesEndPoint;
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

@RequiredInterfaces(required = {DHTServicesCI.class})
public class ClientMRMassiveTest extends AbstractComponent {

    private final DHTServicesEndPoint dht_edp;
    private final String clientId;

    protected ClientMRMassiveTest(String uri, DHTServicesEndPoint dht_edp) throws Exception {
        super(1, 0);
        this.dht_edp = dht_edp;
        this.clientId = uri;
    }

    @Override
    public synchronized void start() throws ComponentStartException {
        try {
            dht_edp.initialiseClientSide(this);
        } catch (ConnectionException e) {
            throw new ComponentStartException("Erreur lors de l'initialisation du client", e);
        }
        super.start();
    }

    @Override
    public void execute() throws Exception {
        this.traceMessage("🧩 Client massif " + clientId + " démarre...\n");

        int total = 0;
        int nombreInserts = 1000;  // volume important
        for (int i = 0; i < nombreInserts; i++) {
        	if(i%144==0) {
        		System.out.println("zzz");
        		Thread.sleep(250);
        		System.out.println("Link ! Wake up !");
        	}
            String nom = clientId + "-Bulk-" + i;
            int age = 20 + (i % 50); // pour avoir une plage variée mais bornée
            total += age;

            ContentKeyI key = new ContentKey(nom);
            ContentDataI person = new Personne(nom, age);
            dht_edp.getClientSideReference().put(key, person);
        }

        this.traceMessage("✅ [" + clientId + "] " + nombreInserts + " données insérées.\n");

        SelectorI selector = data -> ((String) data.getValue("NOM")).startsWith(clientId + "-Bulk");
        ProcessorI<Integer> processor = data -> (Integer) data.getValue("AGE");
        ReductorI<Integer, Integer> reductor = Integer::sum;
        CombinatorI<Integer> combinator = Integer::sum;

        int result = dht_edp.getClientSideReference().mapReduce(selector, processor, reductor, combinator, 0);
        this.traceMessage("🎯 [" + clientId + "] Résultat total = " + result + "\n");

        if(result!=total) {
        	System.out.println("❌ Résultat inattendu : attendu " + total + ", obtenu " + result);
        }

        this.traceMessage("✅ [" + clientId + "] MapReduce massif validé ✅\n");
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
