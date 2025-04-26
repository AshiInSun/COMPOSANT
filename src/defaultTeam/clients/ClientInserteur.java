package defaultTeam.clients;

import fr.sorbonne_u.components.AbstractComponent;
import fr.sorbonne_u.components.annotations.RequiredInterfaces;
import fr.sorbonne_u.components.exceptions.ComponentStartException;
import fr.sorbonne_u.components.exceptions.ConnectionException;
import fr.sorbonne_u.cps.dht_mapreduce.interfaces.content.ContentDataI;
import fr.sorbonne_u.cps.dht_mapreduce.interfaces.content.ContentKeyI;
import fr.sorbonne_u.cps.dht_mapreduce.interfaces.frontend.DHTServicesCI;

import java.util.ArrayList;
import java.util.List;

import defaultTeam.ContentKey;
import defaultTeam.Personne;
import defaultTeam.endpoints.DHTServicesEndPoint;

@RequiredInterfaces(required = {DHTServicesCI.class})
public class ClientInserteur extends AbstractComponent {

    private DHTServicesEndPoint dht_edp;
    private final String clientId;
    private final int NB_VALUES = 400;

    protected ClientInserteur(String uri, DHTServicesEndPoint dht_edp) throws Exception {
        super(1, 0);
        this.dht_edp = dht_edp;
        this.clientId = uri; // utile pour générer des clés uniques et identifiables
       
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
        this.traceMessage("ClientInserteur démarre...\n");
        
        List<ContentKeyI> keys = new ArrayList<>();
        List<ContentDataI> personnes = new ArrayList<>();

        for (int i = 0; i < NB_VALUES; i++) {
            ContentKeyI key = new ContentKey("c" + clientId + "-k" + i);
            ContentDataI person = new Personne("Client" + clientId + "-Person" + i, 20 + i);
            dht_edp.getClientSideReference().put(key, person);

            keys.add(key);
            personnes.add(person);
            this.traceMessage("Put n°" + i + ": " + key.toString() + "\n");
        }
        
        for (int i = 0; i < keys.size(); i++) {
        	this.traceMessage("*** TRY TO GET ***\n");
            ContentKeyI key = keys.get(i);
            ContentDataI expected = personnes.get(i);
            ContentDataI actual = dht_edp.getClientSideReference().get(key);
            this.traceMessage("Get est passé :"+actual+"\n");
            try {
            assert actual != null : "GET null pour clé " + key;
            assert actual.getValue("NOM").equals(expected.getValue("NOM"));
            assert actual.getValue("AGE").equals(expected.getValue("AGE"));
            }catch (AssertionError e) {
            	this.traceMessage("❌ AssertionError : " + e.getMessage() + "\n");
            	System.out.println("❌ AssertionError : " + e.getMessage() + "\n");
            }
            this.traceMessage("[GET OK] " + key + "\n");
        }


        this.traceMessage("ClientInserteur " + clientId + " a fini.\n");
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
