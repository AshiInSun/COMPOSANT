import fr.sorbonne_u.cps.dht_mapreduce.interfaces.content.ContentKeyI;

public class ContentKey implements ContentKeyI {

	private static final long serialVersionUID = 1L;
    private final String key;

    public ContentKey(String key) {
        this.key = key;
    }

    @Override
    public int hashCode() {
        int hashed =  key.hashCode() % 200;
        return (hashed < 0) ? -1 * 200 : hashed;	// Pour eviter que le hash soit negatif
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
