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

class DHTOutboundPort extends AbstractOutboundPort implements DHTServicesCI {
    private static final long serialVersionUID = 1L;

    protected DHTOutboundPort(String uri, ComponentI owner) throws Exception {
        super(uri, DHTServicesCI.class, owner);
    }

    @Override
    public ContentDataI get(ContentKeyI key) throws Exception {
        return ((DHTServicesCI) this.getConnector()).get(key);
    }

    @Override
    public ContentDataI put(ContentKeyI key, ContentDataI value) throws Exception {
        return ((DHTServicesCI) this.getConnector()).put(key, value);
    }

    @Override
    public ContentDataI remove(ContentKeyI key) throws Exception {
        return ((DHTServicesCI) this.getConnector()).remove(key);
    }

    @Override
    public <R extends Serializable, A extends Serializable> A mapReduce(SelectorI selector, ProcessorI<R> processor,
            ReductorI<A, R> reductor, CombinatorI<A> combinator, A initialAcc) throws Exception {
        return ((DHTServicesCI) this.getConnector()).mapReduce(selector, processor, reductor, combinator, initialAcc);
    }
}