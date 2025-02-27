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

class DHTInboundPort extends AbstractInboundPort implements DHTServicesCI {
    private static final long serialVersionUID = 1L;
    private final ComponentI owner;

    protected DHTInboundPort(String uri, ComponentI owner) throws Exception {
        super(uri, DHTServicesCI.class, (ComponentI) owner);
        this.owner = owner;
    }

    @Override
    public ContentDataI get(ContentKeyI key) throws Exception {
        if (this.owner instanceof DHTServicesCI) {
            return ((DHTServicesCI) this.owner).get(key);
        }
        throw new Exception("Composant non compatible avec DHTServicesCI");
    }

    @Override
    public ContentDataI put(ContentKeyI key, ContentDataI value) throws Exception {
        if (this.owner instanceof DHTServicesCI) {
            return ((DHTServicesCI) this.owner).put(key, value);
        }
        throw new Exception("Composant non compatible avec DHTServicesCI");
    }

    @Override
    public ContentDataI remove(ContentKeyI key) throws Exception {
        if (this.owner instanceof DHTServicesCI) {
            return ((DHTServicesCI) this.owner).remove(key);
        }
        throw new Exception("Composant non compatible avec DHTServicesCI");
    }

    @Override
    public <R extends Serializable, A extends Serializable> A mapReduce(SelectorI selector, ProcessorI<R> processor,
            ReductorI<A, R> reductor, CombinatorI<A> combinator, A initialAcc) throws Exception {
        if (selector == null || processor == null || reductor == null || combinator == null || initialAcc == null) {
            throw new IllegalArgumentException("Paramètre(s) de mapReduce null");
        }
        if (this.owner instanceof DHTServicesCI) {
            return ((DHTServicesCI) this.owner).mapReduce(selector, processor, reductor, combinator, initialAcc);
        }
        throw new Exception("Composant non compatible avec DHTServicesCI");
    }
}