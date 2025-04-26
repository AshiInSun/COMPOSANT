package defaultTeam.utils;

import fr.sorbonne_u.cps.dht_mapreduce.interfaces.mapreduce.ParallelMapReduceI.ParallelismPolicyI;

import java.util.*;

public class ValidChordPolicy implements ParallelismPolicyI {
    /**
	 * 
	 */
	private static final long serialVersionUID = 1L;
	private final List<Integer> chordIndices;
    private final Set<String> alreadyCalledUris;

    public ValidChordPolicy(List<Integer> chordIndices) {
        this.chordIndices = new ArrayList<>(chordIndices); // les indices de cordes à contacter
        this.alreadyCalledUris = new HashSet<>();           // pour éviter les doublons
    }

    public List<Integer> getChordsIndices() {
        return chordIndices;
    }

    public boolean removeEndpointCallUri(String uri) {
        return true;
    	//return alreadyCalledUris.add(uri); // retourne true si l'URI n'était pas encore appelé
    }
}
