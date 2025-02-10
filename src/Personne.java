import java.io.Serializable;

import fr.sorbonne_u.cps.dht_mapreduce.interfaces.content.ContentDataI;

public class Personne implements ContentDataI {

	private static final long serialVersionUID = 1L;
	private String nom;
	private int age;
	
	public Personne(String nom, int age) {
		this.nom = nom;
		this.age = age;
	}

	@Override
	public Serializable	getValue(String attributeName){
		switch (attributeName) {
			case "NOM":
				return this.nom;
			case "AGE":
				return this.age;
			default:
				throw new IllegalArgumentException("unknown attribute for Person: " + attributeName);	
			
				
		}
	}

}
