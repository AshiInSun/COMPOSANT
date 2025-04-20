package defaultTeam;

import fr.sorbonne_u.cps.dht_mapreduce.interfaces.management.LoadPolicyI;

public class LoadPolicy implements LoadPolicyI {
    private static final long serialVersionUID = 1L;

    protected final int splitThreshold;
    protected final int mergeThreshold;

    public LoadPolicy(int splitThreshold, int mergeThreshold) {
        assert splitThreshold > mergeThreshold : 
            "Le seuil de split doit être supérieur à celui de merge.";
        this.splitThreshold = splitThreshold;
        this.mergeThreshold = mergeThreshold;
    }
    
    @Override
    public String toString() {
        return "[LoadPolicy: split > " + splitThreshold + ", merge < " + mergeThreshold + "]";
    }

	@Override
	public boolean shouldSplitInTwoAdjacentNodes(int currentSize) {
		return currentSize > splitThreshold;
	}

	@Override
	public boolean shouldMergeWithNextNode(int thisNodeCurrentSize, int nextNodeCurrentSize) {
		return thisNodeCurrentSize + nextNodeCurrentSize < mergeThreshold;
	}
}
