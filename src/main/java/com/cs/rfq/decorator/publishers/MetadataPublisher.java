package com.cs.rfq.decorator.publishers;

import com.cs.rfq.decorator.Rfq;
import com.cs.rfq.decorator.extractors.RfqMetadataFieldNames;

import java.io.IOException;
import java.util.Map;

/**
 * Simple interface for different metadata publishers to implement
 */
public interface MetadataPublisher {
    void publishMetadata(Rfq rfq, Map<RfqMetadataFieldNames, Object> metadata) throws IOException;
}
