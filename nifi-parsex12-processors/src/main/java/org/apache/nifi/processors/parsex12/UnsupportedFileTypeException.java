package org.apache.nifi.processors.parsex12;

public class UnsupportedFileTypeException extends Exception {
    public UnsupportedFileTypeException(String message) {
        super("File type not supported: " + message);
    }
}
