import java.io.Serializable;

import fr.sorbonne_u.components.connectors.AbstractConnector;
import fr.sorbonne_u.cps.dht_mapreduce.interfaces.content.ContentDataI;
import fr.sorbonne_u.cps.dht_mapreduce.interfaces.content.ContentKeyI;
import fr.sorbonne_u.cps.dht_mapreduce.interfaces.frontend.DHTServicesCI;
import fr.sorbonne_u.cps.dht_mapreduce.interfaces.mapreduce.CombinatorI;
import fr.sorbonne_u.cps.dht_mapreduce.interfaces.mapreduce.ProcessorI;
import fr.sorbonne_u.cps.dht_mapreduce.interfaces.mapreduce.ReductorI;
import fr.sorbonne_u.cps.dht_mapreduce.interfaces.mapreduce.SelectorI;

class DHTConnector extends AbstractConnector implements DHTServicesCI {
    @Override
    public ContentDataI get(ContentKeyI key) throws Exception {
        return ((DHTServicesCI) this.offering).get(key);
    }

    @Override
    public ContentDataI put(ContentKeyI key, ContentDataI value) throws Exception {
        return ((DHTServicesCI) this.offering).put(key, value);
    }

    @Override
    public ContentDataI remove(ContentKeyI key) throws Exception {
        return ((DHTServicesCI) this.offering).remove(key);
    }

    @Override
    public <R extends Serializable, A extends Serializable> A mapReduce(SelectorI selector, ProcessorI<R> processor,
            ReductorI<A, R> reductor, CombinatorI<A> combinator, A initialAcc) throws Exception {
        return ((DHTServicesCI) this.offering).mapReduce(selector, processor, reductor, combinator, initialAcc);
    }
}