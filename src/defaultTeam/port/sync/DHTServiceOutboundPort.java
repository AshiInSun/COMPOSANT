package defaultTeam.port.sync;

import java.io.Serializable;

import fr.sorbonne_u.components.ComponentI;
import fr.sorbonne_u.components.ports.AbstractOutboundPort;
import fr.sorbonne_u.cps.dht_mapreduce.interfaces.content.ContentDataI;
import fr.sorbonne_u.cps.dht_mapreduce.interfaces.content.ContentKeyI;
import fr.sorbonne_u.cps.dht_mapreduce.interfaces.frontend.DHTServicesCI;
import fr.sorbonne_u.cps.dht_mapreduce.interfaces.mapreduce.CombinatorI;
import fr.sorbonne_u.cps.dht_mapreduce.interfaces.mapreduce.ProcessorI;
import fr.sorbonne_u.cps.dht_mapreduce.interfaces.mapreduce.ReductorI;
import fr.sorbonne_u.cps.dht_mapreduce.interfaces.mapreduce.SelectorI;

public class DHTServiceOutboundPort extends AbstractOutboundPort implements DHTServicesCI {
    private static final long serialVersionUID = 1L;

    public DHTServiceOutboundPort(String uri, ComponentI owner) throws Exception {
        super(uri, DHTServicesCI.class, owner);
    }
    
    public DHTServiceOutboundPort(ComponentI owner) throws Exception {
        super(DHTServicesCI.class, owner);
    }

    @Override
    public ContentDataI get(ContentKeyI key) throws Exception {
    	if (this.connected()) {
            return ((DHTServicesCI) this.getConnector()).get(key);
        } else {
            throw new Exception("DHTOutboundPort : no connexion");
        }
    }

    @Override
    public ContentDataI put(ContentKeyI key, ContentDataI value) throws Exception {
    	if (this.connected()) {
            return ((DHTServicesCI) this.getConnector()).put(key, value);
        } else {
            throw new Exception("DHTOutboundPort : no connexion");
        }
    }

    @Override
    public ContentDataI remove(ContentKeyI key) throws Exception {
    	if (this.connected()) {
            return ((DHTServicesCI) this.getConnector()).remove(key);
        } else {
            throw new Exception("DHTOutboundPort : Pas de connexion établie !");
        }
    }

    @Override
    public <R extends Serializable, A extends Serializable> A mapReduce(
		SelectorI selector, 
		ProcessorI<R> processor,
        ReductorI<A, R> reductor, 
        CombinatorI<A> combinator,
        A initialAcc) throws Exception {
    	
    	if (this.connected()) {
            return ((DHTServicesCI) this.getConnector()).mapReduce(selector, processor, reductor, combinator, initialAcc);
        } else {
            throw new Exception("DHTOutboundPort : Pas de connexion établie !");
        }
    }
}