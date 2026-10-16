package com.pfizer.bridge.common;

import com.pfizer.bridge.common.EnvConfig.ConfigException;
import org.w3c.dom.Document;
import org.w3c.dom.NodeList;
import org.xml.sax.InputSource;
import org.xml.sax.SAXException;

import javax.xml.XMLConstants;
import javax.xml.parsers.DocumentBuilder;
import javax.xml.parsers.DocumentBuilderFactory;
import javax.xml.parsers.ParserConfigurationException;
import javax.xml.transform.dom.DOMSource;
import javax.xml.validation.Schema;
import javax.xml.validation.SchemaFactory;
import javax.xml.validation.Validator;
import java.io.IOException;
import java.io.StringReader;
import java.nio.file.Path;
import java.util.Optional;

/**
 * Extracts PSUniqueExceptionID and, if an XSD (RAExternalExceptionHandling.xsd) is configured, validates the payload.
 * Parsing is hardened against XXE. Inspection never blocks forwarding; results become SQS message attributes.
 */
public final class XmlInspector {

    public record Result(Optional<String> exceptionId, Optional<Boolean> schemaValid, String validationError) {
    }

    private final DocumentBuilderFactory dbf;
    private final Optional<Schema> schema;

    public XmlInspector(Optional<Path> xsdPath) {
        try {
            dbf = DocumentBuilderFactory.newInstance();
            dbf.setNamespaceAware(true);
            dbf.setFeature("http://apache.org/xml/features/disallow-doctype-decl", true);
            dbf.setFeature(XMLConstants.FEATURE_SECURE_PROCESSING, true);
            dbf.setXIncludeAware(false);
            dbf.setExpandEntityReferences(false);
        } catch (ParserConfigurationException e) {
            throw new IllegalStateException(e);
        }
        schema = xsdPath.map(XmlInspector::loadSchema);
    }

    private static Schema loadSchema(Path path) {
        try {
            SchemaFactory factory = SchemaFactory.newInstance(XMLConstants.W3C_XML_SCHEMA_NS_URI);
            factory.setProperty(XMLConstants.ACCESS_EXTERNAL_DTD, "");
            // Allow local xs:include/xs:import next to the main XSD, nothing remote
            factory.setProperty(XMLConstants.ACCESS_EXTERNAL_SCHEMA, "file");
            return factory.newSchema(path.toFile());
        } catch (SAXException e) {
            throw new ConfigException("Cannot load XSD " + path + ": " + e.getMessage(), e);
        }
    }

    public Result inspect(String body) {
        Document doc;
        try {
            DocumentBuilder builder = dbf.newDocumentBuilder();
            builder.setErrorHandler(null);
            doc = builder.parse(new InputSource(new StringReader(body)));
        } catch (ParserConfigurationException | SAXException | IOException e) {
            return new Result(Optional.empty(), schema.map(s -> false), "Not well-formed XML: " + e.getMessage());
        }

        NodeList ids = doc.getElementsByTagNameNS("*", "PSUniqueExceptionID");
        Optional<String> exceptionId = ids.getLength() > 0
                ? Optional.ofNullable(ids.item(0).getTextContent()).map(String::trim).filter(s -> !s.isEmpty())
                : Optional.empty();

        if (schema.isEmpty()) {
            return new Result(exceptionId, Optional.empty(), null);
        }
        try {
            Validator validator = schema.get().newValidator();
            validator.setProperty(XMLConstants.ACCESS_EXTERNAL_DTD, "");
            validator.setProperty(XMLConstants.ACCESS_EXTERNAL_SCHEMA, "");
            validator.validate(new DOMSource(doc));
            return new Result(exceptionId, Optional.of(true), null);
        } catch (SAXException | IOException e) {
            return new Result(exceptionId, Optional.of(false), e.getMessage());
        }
    }
}
