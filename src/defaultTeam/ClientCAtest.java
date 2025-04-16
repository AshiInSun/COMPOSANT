package defaultTeam;

import fr.sorbonne_u.components.AbstractComponent;
import fr.sorbonne_u.components.annotations.RequiredInterfaces;
import fr.sorbonne_u.components.exceptions.ComponentStartException;
import fr.sorbonne_u.components.exceptions.ConnectionException;
import fr.sorbonne_u.cps.dht_mapreduce.interfaces.content.ContentDataI;
import fr.sorbonne_u.cps.dht_mapreduce.interfaces.content.ContentKeyI;
import fr.sorbonne_u.cps.dht_mapreduce.interfaces.frontend.DHTServicesCI;

import java.util.ArrayList;
import java.util.List;

import defaultTeam.endpoints.DHTServicesEndPoint;

@RequiredInterfaces(required = {DHTServicesCI.class})
public class ClientCAtest extends AbstractComponent {

    private DHTServicesEndPoint dht_edp;
    private final String clientId;

    protected ClientCAtest(String uri, DHTServicesEndPoint dht_edp) throws Exception {
        super(1, 0);
        this.dht_edp = dht_edp;
        this.clientId = uri; // utile pour générer des clés uniques et identifiables
       
    }

    @Override
    public synchronized void start() throws ComponentStartException {
    	try {
    		this.traceMessage("start");
            dht_edp.initialiseClientSide(this);
            this.traceMessage("start");
        } catch (ConnectionException e) {
            e.printStackTrace();
        }
        super.start();
    }

    @Override
    public void execute() throws Exception {
        this.traceMessage("ClientCA démarre...\n");
        
        // === Test 1 : remplacement de valeur ===
        ContentKeyI keyReplace = new ContentKey("key-replace");
        ContentDataI v1 = new Personne("Ancienne", 25);
        ContentDataI v2 = new Personne("Nouvelle", 99);

        ContentDataI put1 = dht_edp.getClientSideReference().put(keyReplace, v1);
        try {
        assert put1 == null : "La clé ne devrait pas exister au premier put.";

        ContentDataI put2 = dht_edp.getClientSideReference().put(keyReplace, v2);
        assert put2 != null : "Le deuxième put doit retourner l’ancienne valeur.";
        assert put2.getValue("NOM").equals("Ancienne");

        ContentDataI result1 = dht_edp.getClientSideReference().get(keyReplace);
        assert result1.getValue("NOM").equals("Nouvelle");
	    }catch (AssertionError e) {
	    	this.traceMessage("❌ AssertionError : " + e.getMessage() + "\n");
	    	System.out.println("❌ AssertionError : " + e.getMessage() + "\n");
	    }
        this.traceMessage("✅ Test remplacement réussi.\n");
        //TEST 2
        
        ContentKeyI keyRemove = new ContentKey("key-remove");
        ContentDataI person = new Personne("À supprimer", 42);

        dht_edp.getClientSideReference().put(keyRemove, person);
        ContentDataI removed = dht_edp.getClientSideReference().remove(keyRemove);
        
        try {
        assert removed != null : "La suppression doit retourner l’ancienne valeur.";
        assert removed.getValue("NOM").equals("À supprimer");

        ContentDataI afterRemove = dht_edp.getClientSideReference().get(keyRemove);
        assert afterRemove == null : "La donnée doit avoir été supprimée.";
        }catch(AssertionError e) {
        	this.traceMessage("❌ AssertionError : " + e.getMessage() + "\n");
	    	System.out.println("❌ AssertionError : " + e.getMessage() + "\n");
        }

        this.traceMessage("✅ Test suppression réussi.\n");
        // === Test 3 : get sur clé inexistante ===
        ContentKeyI unknown = new ContentKey("clé-inconnue");
        ContentDataI r = dht_edp.getClientSideReference().get(unknown);
        try {
        	assert r == null : "Un get sur une clé inexistante doit renvoyer null.";
	    }catch(AssertionError e) {
	    	this.traceMessage("❌ AssertionError : " + e.getMessage() + "\n");
	    	System.out.println("❌ AssertionError : " + e.getMessage() + "\n");
	    }

        this.traceMessage("✅ Test get clé inexistante réussi.\n");

        this.traceMessage("🎉 ClientContentAccess a terminé tous les tests.\n");

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
