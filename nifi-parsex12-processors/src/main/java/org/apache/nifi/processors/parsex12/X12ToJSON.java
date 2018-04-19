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

    private static final Relationship REL_FAILURE = new Relationship.Builder()
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
        final FlowFile flowFile = session.get();
        if (flowFile == null) {
            return;
        }

        final FlowFile flowFileClone = session.clone(flowFile);

        session.read(flowFile, inputStream -> session.write(flowFileClone, outputStream -> {
            try {

                byte[] byteArray = IOUtils.toByteArray(inputStream);
                inputStream.close();

                InputStream typeStream = new ByteArrayInputStream(byteArray);
                GetX12FileType getX12FileType = new GetX12FileType(typeStream);
                typeStream.close();

                InputStream readerStream = new ByteArrayInputStream(byteArray);
                X12Reader x12Reader = new X12Reader(X12Reader.FileType.ANSI835_5010_X221, readerStream);
                readerStream.close();

                outputStream.write(x12Reader.getLoop().toJson().getBytes());
            } catch (Exception e) {
                getLogger().error("Exception: " + e.getMessage());
                session.transfer(flowFile, REL_FAILURE);
            }
        }));

        session.transfer(flowFileClone, REL_SUCCESS);
        session.remove(flowFile);
        session.commit();
    }
}
