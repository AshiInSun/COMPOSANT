package defaultTeam;
import fr.sorbonne_u.cps.dht_mapreduce.interfaces.content.ContentKeyI;

public class ContentKey implements ContentKeyI {

	private static final long serialVersionUID = 1L;
    private final String key;

    public ContentKey(String key) {
        this.key = key;
    }

    @Override
    public int hashCode() {
        return Math.floorMod(key.hashCode(), 200);
    }


    @Override
    public boolean equals(Object obj) {
        if (this == obj) return true;
        if (obj == null || getClass() != obj.getClass()) return false;
        ContentKey other = (ContentKey) obj;
        return key.equals(other.key);
    }
    
    public String getKey() {
    	return this.key;
    }
}
