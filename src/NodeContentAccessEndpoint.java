import fr.sorbonne_u.components.endpoints.EndPointI;
import fr.sorbonne_u.cps.dht_mapreduce.interfaces.content.ContentAccessSyncI;
import fr.sorbonne_u.cps.dht_mapreduce.interfaces.content.ContentDataI;
import fr.sorbonne_u.cps.dht_mapreduce.interfaces.content.ContentKeyI;

public class NodeContentAccessEndpoint implements EndPointI<ContentAccessSyncI>{
	
	private final Node node;

    public NodeContentAccessEndpoint(Node node) {
        this.node = node;
    }

    public ContentDataI getSync(String computationURI, ContentKeyI key) throws Exception {
        return node.getSync(computationURI, key);
    }

    public ContentDataI putSync(String computationURI, ContentKeyI key, ContentDataI value) throws Exception {
        return node.putSync(computationURI, key, value);
    }

    public ContentDataI removeSync(String computationURI, ContentKeyI key) throws Exception {
        return node.removeSync(computationURI, key);
    }
}
