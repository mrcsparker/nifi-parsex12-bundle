package org.apache.nifi.processors.parsex12;

import com.imsweb.x12.reader.X12Reader;
import org.apache.commons.io.IOUtils;
import org.apache.nifi.annotation.documentation.CapabilityDescription;
import org.apache.nifi.annotation.documentation.Tags;
import org.apache.nifi.components.PropertyDescriptor;
import org.apache.nifi.flowfile.FlowFile;
import org.apache.nifi.processor.*;
import org.apache.nifi.processor.exception.ProcessException;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.ByteArrayInputStream;
import java.io.InputStream;
import java.util.*;

@Tags({"EDI", "X12", "JSON"})
@CapabilityDescription("Transform EDI X12 into JSON.")
public class X12ToJSON extends AbstractProcessor {

    static Logger LOGGER = LoggerFactory.getLogger(X12ToJSON.class);

    private List<PropertyDescriptor> properties;
    private Set<Relationship> relationships;

    public static final Relationship REL_SUCCESS = new Relationship.Builder()
            .name("success")
            .description("success")
            .build();

    public static final Relationship REL_FAILURE = new Relationship.Builder()
            .name("failure")
            .description("failure")
            .build();


    @Override
    public void init(final ProcessorInitializationContext context) {

        final List<PropertyDescriptor> properties = new ArrayList<>();
        this.properties = properties;

        Set<Relationship> relationships = new HashSet<>();
        relationships.add(REL_SUCCESS);
        relationships.add(REL_FAILURE);
        this.relationships = Collections.unmodifiableSet(relationships);
    }

    @Override
    public Set<Relationship> getRelationships() {
        return this.relationships;
    }

    @Override
    public final List<PropertyDescriptor> getSupportedPropertyDescriptors() {
        return this.properties;
    }

    @Override
    public void onTrigger(ProcessContext processContext, ProcessSession session) throws ProcessException {
        FlowFile flowFile = session.get();
        if (flowFile == null) {
            return;
        }

        try {
            flowFile = session.write(flowFile, (in, out) -> {
                byte[] byteArray = IOUtils.toByteArray(in);

                try (final InputStream typeStream = new ByteArrayInputStream(byteArray);
                     final InputStream readerStream = new ByteArrayInputStream(byteArray)) {

                    GetX12FileType getX12FileType = new GetX12FileType(typeStream);
                    X12Reader.FileType fileType = getX12FileType.getFileFormat();
                    X12Reader x12Reader = new X12Reader(fileType, readerStream);

                    out.write(x12Reader.getLoop().toJson().getBytes());
                }
            });
        } catch (final ProcessException pe) {
            session.transfer(flowFile, REL_FAILURE);
            return;
        }


        session.transfer(flowFile, REL_SUCCESS);
    }
}
