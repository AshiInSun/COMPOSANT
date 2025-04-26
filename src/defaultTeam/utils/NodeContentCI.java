package defaultTeam.utils;

import java.util.HashMap;
import java.util.Map;

import defaultTeam.endpoints.BCMAsyncContentNodeCompositeEndPoint;
import fr.sorbonne_u.cps.dht_mapreduce.interfaces.content.ContentDataI;
import fr.sorbonne_u.cps.dht_mapreduce.interfaces.content.ContentKeyI;
import fr.sorbonne_u.cps.dht_mapreduce.interfaces.management.DHTManagementI.NodeContentI;
import fr.sorbonne_u.cps.mapreduce.utils.IntInterval;

public class NodeContentCI implements NodeContentI {

	/**
	 * 
	 */
	private static final long serialVersionUID = 1L;
	
	protected final Map<ContentKeyI, ContentDataI> content;
    protected final IntInterval interval;
    protected final BCMAsyncContentNodeCompositeEndPoint server_edp;
    
    public NodeContentCI(Map<ContentKeyI, ContentDataI> content, IntInterval interval) {
        // Deep copy si nécessaire
        this.content = new HashMap<>(content);
        this.interval = interval;
        this.server_edp = null;
    }
    public NodeContentCI(Map<ContentKeyI, ContentDataI> content, IntInterval interval, BCMAsyncContentNodeCompositeEndPoint server_edp) {
        // Deep copy si nécessaire
        this.content = new HashMap<>(content);
        this.interval = interval;
        this.server_edp = server_edp;
    }
    
    public Map<ContentKeyI, ContentDataI> getContent() {
        return content;
    }

    public IntInterval getInterval() {
    	return interval;
    }
    
    public BCMAsyncContentNodeCompositeEndPoint getServer_edp() {
    	return this.server_edp;
    }

    @Override
    public String toString() {
        return "[NodeContent: Interval = (" + interval.first() + "," + interval.last()+ "), Size = " + content.size() + "]";
    }

}
