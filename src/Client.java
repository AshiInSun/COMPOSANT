import fr.sorbonne_u.cps.dht_mapreduce.interfaces.content.ContentDataI;
import fr.sorbonne_u.cps.dht_mapreduce.interfaces.content.ContentKeyI;
import fr.sorbonne_u.cps.dht_mapreduce.interfaces.mapreduce.CombinatorI;
import fr.sorbonne_u.cps.dht_mapreduce.interfaces.mapreduce.ProcessorI;
import fr.sorbonne_u.cps.dht_mapreduce.interfaces.mapreduce.ReductorI;
import fr.sorbonne_u.cps.dht_mapreduce.interfaces.mapreduce.SelectorI;

public class Client {
	public static void main(String[] args) {
       try { 
	        // Initialisation de la façade
	        Facade facade = new Facade(2);
	
	        // Création de Personnes et de leur clef associée
	        ContentKeyI k1 = new ContentKey("123");
	        ContentKeyI k2 = new ContentKey("nextnode");
	        ContentKeyI k3 = new ContentKey("789");
	        ContentKeyI k4 = new ContentKey("123");
	
	        ContentDataI p1 = new Personne("Alpha", 10);
	        ContentDataI p2 = new Personne("Beta", 16);
	        ContentDataI p3 = new Personne("Delta", 35);
	        ContentDataI p4 = new Personne("Omega", 82);
	
	        // Ajout des données dans la table
	        System.out.println("Insertions de donnée...\n");
	        facade.put(k1, p1);
	        facade.put(k2, p2);
	        facade.put(k3, p3);
	
	        
	        // Récupération des données
	        System.out.println("Récupération des données...");
	        
	        ContentDataI result1 = facade.get(k1);
	        System.out.println("Donnée pour k1: " + result1.getValue("NOM") + ", " + result1.getValue("AGE"));
	
	        ContentDataI result2 = facade.get(k2);
	        System.out.println("Donnée pour k2: " + result2.getValue("NOM") + ", " + result2.getValue("AGE"));
	
	        ContentDataI result3 = facade.get(k3);
	        System.out.println("Donnée pour k3: " + result3.getValue("NOM") + ", " + result3.getValue("AGE"));
	
	        
	        // Moyenne des ages avec mapReduce
	        System.out.println("\nUtilisation de mapReduce pour calculer l'age moyen");
	        SelectorI selector = data -> true;
	        ProcessorI<Integer> processor = data -> {
	        	if (data instanceof Personne) {
	        		return (Integer) data.getValue("AGE");
	        	}
	        	return 0;
	        };
	        ReductorI<int[], Integer> reductor = (acc, age) -> new int[]{acc[0] + age, acc[1] + 1};
	        CombinatorI<int[]> combinator = (acc1, acc2) -> new int[] {acc1[0] + acc2[0], acc1[1] + acc2[1]};
	        int[] initialAcc = new int[] {0, 0};
	        
	        int[] res = facade.mapReduce(selector, processor, reductor, combinator, initialAcc);
	        double ageMoyen = (res[1] == 0) ? 0 : (double)res[0] / res[1];	        
	        System.out.println("L'age moyen est: "+ ageMoyen);
	        
	        
	        // Suppression d'une donnée
	        System.out.println("\nSuppression de la donnée pour k2...");
	        ContentDataI removedResult1 = facade.remove(k2);
	        System.out.println("Donnée: " + removedResult1.getValue("NOM") + ", " + removedResult1.getValue("AGE") + " a été supprimée");
	        
	        
	        // Verification de la suppression
	        ContentDataI removedResult2 = facade.get(k2);
	        System.out.println("Donnée pour key2 après suppression: " + (removedResult2 == null ? "null" : removedResult2));
	        
	        
	        // Remplacement d'une donnée
	        System.out.println("\nRemplacement de la donnée pour k1...");
	        ContentDataI replacedResult = facade.put(k4, p4);
	        System.out.println("Donnée: " + replacedResult.getValue("NOM") + ", " + replacedResult.getValue("AGE") + " a été remplacée");
	        ContentDataI result4 = facade.get(k4);
	        System.out.println("Donnée: " + result4.getValue("NOM") + ", " + result4.getValue("AGE") + " l'a remplacée\n");   
	        
	        int[] res2 = facade.mapReduce(selector, processor, reductor, combinator, initialAcc);
	        double ageMoyen2 = (res2[1] == 0) ? 0 : (double)res2[0] / res2[1];	        
	        System.out.println("L'age moyen apres ces modifications est: "+ ageMoyen2);
	        
	    } catch (Exception e) {
	    	e.printStackTrace();
	    }
	}
}