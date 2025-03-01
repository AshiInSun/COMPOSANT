import java.io.Serializable;

import fr.sorbonne_u.components.connectors.AbstractConnector;
import fr.sorbonne_u.cps.dht_mapreduce.interfaces.content.ContentAccessSyncCI;
import fr.sorbonne_u.cps.dht_mapreduce.interfaces.content.ContentDataI;
import fr.sorbonne_u.cps.dht_mapreduce.interfaces.content.ContentKeyI;
import fr.sorbonne_u.cps.dht_mapreduce.interfaces.frontend.DHTServicesCI;
import fr.sorbonne_u.cps.dht_mapreduce.interfaces.mapreduce.CombinatorI;
import fr.sorbonne_u.cps.dht_mapreduce.interfaces.mapreduce.ProcessorI;
import fr.sorbonne_u.cps.dht_mapreduce.interfaces.mapreduce.ReductorI;
import fr.sorbonne_u.cps.dht_mapreduce.interfaces.mapreduce.SelectorI;

// TODO: mapReduce
//       et eventuellement les methodes clearComputation si on les ajoute

class DHTConnector extends AbstractConnector implements DHTServicesCI {
    @Override
    public ContentDataI get(ContentKeyI key) throws Exception {
    	ContentDataI data = ((ContentAccessSyncCI) this.offering).getSync(offeringPortURI, key);
    	((ContentAccessSyncCI) this.offering).clearComputation(offeringPortURI);
    	
    	return data;
    }

    @Override
    public ContentDataI put(ContentKeyI key, ContentDataI value) throws Exception {
    	ContentDataI previousData = ((ContentAccessSyncCI) this.offering).putSync(offeringPortURI, key, value);
    	((ContentAccessSyncCI) this.offering).clearComputation(offeringPortURI);
    	
    	return previousData;
    }

    @Override
    public ContentDataI remove(ContentKeyI key) throws Exception {
    	ContentDataI previousData = ((ContentAccessSyncCI) this.offering).removeSync(offeringPortURI, key);
    	((ContentAccessSyncCI) this.offering).clearComputation(offeringPortURI);
    	
    	return previousData;
    }

    @Override
    public <R extends Serializable, A extends Serializable> A mapReduce(SelectorI selector, ProcessorI<R> processor,
            ReductorI<A, R> reductor, CombinatorI<A> combinator, A initialAcc) throws Exception {
    	
    	// TODO
    	// Reprendre la methode mapReduce de la classe Facade mais avec BCM
    	
        return null;
    }
}