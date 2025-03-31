package defaultTeam.port.sync;

import java.io.Serializable;

import defaultTeam.FacadeAsyncComponent;
import defaultTeam.old.FacadeComponent;
import fr.sorbonne_u.components.ComponentI;
import fr.sorbonne_u.components.ports.AbstractInboundPort;
import fr.sorbonne_u.cps.dht_mapreduce.interfaces.content.ContentDataI;
import fr.sorbonne_u.cps.dht_mapreduce.interfaces.content.ContentKeyI;
import fr.sorbonne_u.cps.dht_mapreduce.interfaces.frontend.DHTServicesCI;
import fr.sorbonne_u.cps.dht_mapreduce.interfaces.mapreduce.CombinatorI;
import fr.sorbonne_u.cps.dht_mapreduce.interfaces.mapreduce.ProcessorI;
import fr.sorbonne_u.cps.dht_mapreduce.interfaces.mapreduce.ReductorI;
import fr.sorbonne_u.cps.dht_mapreduce.interfaces.mapreduce.SelectorI;

public class DHTServiceInboundPort extends AbstractInboundPort implements DHTServicesCI {
    private static final long serialVersionUID = 1L;

    public DHTServiceInboundPort(String uri, ComponentI owner) throws Exception {
        super(uri, DHTServicesCI.class, (ComponentI) owner);
    }

	@Override
	public ContentDataI get(ContentKeyI key) throws Exception {
		return this.getOwner().handleRequest(o -> {
	        try {
	            return ((FacadeAsyncComponent) o).get(key);
	        } catch (Exception e) {
	            e.printStackTrace();
	            return null; 
	        }
	    });
	}

	@Override
	public ContentDataI put(ContentKeyI key, ContentDataI value) throws Exception {
		return this.getOwner().handleRequest(o -> {
	        try {
	        	return ((FacadeAsyncComponent) this.owner).put(key, value);
	        } catch (Exception e) {
	            e.printStackTrace();
	            return null; 
	        }
	    });
	}

	@Override
	public ContentDataI remove(ContentKeyI key) throws Exception {
		return this.getOwner().handleRequest(o -> {
	        try {
	        	return ((FacadeAsyncComponent) this.owner).remove(key);
	        } catch (Exception e) {
	            e.printStackTrace();
	            return null; 
	        }
	    });
	}

	@Override
	public <R extends Serializable, A extends Serializable> A mapReduce(
		SelectorI selector, 
		ProcessorI<R> processor,
		ReductorI<A, R> reductor, 
		CombinatorI<A> combinator, 
		A initialAcc) throws Exception {
		return this.getOwner().handleRequest(o -> {
	        try {
	        	return ((FacadeAsyncComponent) this.owner).mapReduce(selector, processor, reductor, combinator, initialAcc);
	        } catch (Exception e) {
	            e.printStackTrace();
	            return null; 
	        }
	    });	
	}

    
}