package org.apache.nifi.processors.parsex12;

import com.imsweb.x12.reader.X12Reader;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.BufferedReader;
import java.io.IOException;
import java.io.InputStream;
import java.io.InputStreamReader;
import java.util.ArrayList;
import java.util.List;
import java.util.Scanner;
import java.util.regex.Pattern;
import java.util.stream.Collectors;

public class GetX12FileType {

    static Logger LOGGER = LoggerFactory.getLogger(GetX12FileType.class);

    private static final int HEADER_LENGTH = 106;
    private static final int SEGMENT_POSITION = 105;
    private static final int ELEMENT_POSITION = 3;
    private final BufferedReader inputReader;

    private Character segmentSeparator;
    private Character elementSeparator;

    private String idCode;
    private String versionCode;
    private String releaseCode;

    public GetX12FileType(InputStream inputStream) throws IOException {
        this.inputReader = new BufferedReader(new InputStreamReader(inputStream));
        init();
    }

    private void init() throws IOException {
        parseHeader();
        Scanner scanner = new Scanner(inputReader);
        scanner.useDelimiter(Pattern.quote(segmentSeparator.toString()));

        while(scanner.hasNext()) {
            String segment = scanner.next().trim();
            String[] elements = segment.split(Pattern.quote(elementSeparator.toString()));
            switch (elements[0]) {
                case "ISA":
                    versionCode = elements[12];
                    break;
                case "GS":
                    releaseCode = elements[8];
                    break;
                case "ST":
                    LOGGER.info("{}", elements);
                    idCode = elements[1];
                    return;
            }
        }

    }

    public String getIdCode() {
        return idCode;
    }

    public String getVersionCode() {
        return versionCode;
    }

    public String getReleaseCode() {
        return releaseCode;
    }

    public X12Reader.FileType getFileFormat() throws UnsupportedFileTypeException {
        String f = idCode + "." + releaseCode;

        LOGGER.info("{}", f);

        switch (f) {
            case "835.005010X221A1": return X12Reader.FileType.ANSI835_5010_X221;
            case "837.005010X222A1": return X12Reader.FileType.ANSI837_5010_X222;
            default: throw new UnsupportedFileTypeException(idCode + " : " + versionCode + " : " + releaseCode);
        }
    }

    private String parseHeader() throws IOException {
        char[] buf = new char[HEADER_LENGTH];

        int size = inputReader.read(buf);
        if (size != HEADER_LENGTH) {
            throw new IOException("Size is not " + HEADER_LENGTH + " but " + size);
        }

        segmentSeparator = buf[SEGMENT_POSITION];
        elementSeparator = buf[ELEMENT_POSITION];

        return testIsaSegment(buf);
    }

    private String testIsaSegment(char[] buf) throws IOException {
        List<String> l = new ArrayList<>();

        String isaSegment = new String(buf);

        Scanner scanner = new Scanner(isaSegment.substring(0, SEGMENT_POSITION));
        scanner.useDelimiter(Pattern.quote(elementSeparator.toString()));
        while (scanner.hasNext()) {
            l.add(scanner.next());
        }
        scanner.close();
        if (!l.get(0).equals("ISA")) {
            throw new IOException("Not valid file format. Got " + l.get(0) + " instead of ISA. " + elementSeparator.toString());
        }
        return l.stream().collect(Collectors.joining(elementSeparator.toString()));
    }


}
