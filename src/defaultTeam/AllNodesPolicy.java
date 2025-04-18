package defaultTeam;

import fr.sorbonne_u.cps.dht_mapreduce.interfaces.mapreduce.ParallelMapReduceI.ParallelismPolicyI;

public class AllNodesPolicy implements ParallelismPolicyI {
    /**
	 * 
	 */
	private static final long serialVersionUID = 1L;

	public boolean apply(String nodeId) {
        return true;  // Tous les nœuds participent
    }
}
