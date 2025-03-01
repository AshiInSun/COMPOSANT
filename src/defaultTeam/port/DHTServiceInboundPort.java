package defaultTeam.port;

import java.io.Serializable;
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
    private final ComponentI owner;

    public DHTServiceInboundPort(String uri, ComponentI owner) throws Exception {
        super(uri, DHTServicesCI.class, (ComponentI) owner);
        this.owner = owner;
    }

	@Override
	public ContentDataI get(ContentKeyI key) throws Exception {
		return ((DHTServicesCI) this.owner).get(key);
	}

	@Override
	public ContentDataI put(ContentKeyI key, ContentDataI value) throws Exception {
		return ((DHTServicesCI) this.owner).put(key, value);
	}

	@Override
	public ContentDataI remove(ContentKeyI key) throws Exception {
		return ((DHTServicesCI) this.owner).remove(key);
	}

	@Override
	public <R extends Serializable, A extends Serializable> A mapReduce(
		SelectorI selector, 
		ProcessorI<R> processor,
		ReductorI<A, R> reductor, 
		CombinatorI<A> combinator, 
		A initialAcc) throws Exception {
		
		return ((DHTServicesCI) this.owner).mapReduce(selector, processor, reductor, combinator, initialAcc);
	}

    
}