package io.rowweave.build;

import org.junit.jupiter.api.Test;
import org.w3c.dom.Document;
import org.w3c.dom.Element;
import org.w3c.dom.Node;
import org.w3c.dom.NodeList;

import javax.xml.parsers.DocumentBuilderFactory;
import java.io.File;

import static org.junit.jupiter.api.Assertions.assertEquals;

/** The build configuration is part of the test evidence contract, so report directories cannot mix across runs again. */
class SurefireReportIsolationPomTest {

    @Test
    void defaultReportsAreRunScopedWithFilesystemSafeTimestamp() throws Exception {
        Element project = pom().getDocumentElement();
        Element properties = child(project, "properties");
        assertEquals("default", text(properties, "rowweave.test.reportSet"));
        assertEquals("yyyyMMdd-HHmmss-SSS", text(properties, "maven.build.timestamp.format"));

        Element surefire = plugin(child(project, "build"), "maven-surefire-plugin");
        assertEquals("${project.build.directory}/surefire-reports/"
                        + "${rowweave.test.reportSet}-${maven.build.timestamp}",
                text(child(surefire, "configuration"), "reportsDirectory"));
    }

    @Test
    void clusterAndProfileRunsHaveExplicitReportSetsAndSelection() throws Exception {
        Element profiles = child(pom().getDocumentElement(), "profiles");
        Element cluster = profile(profiles, "cluster-isolated");
        assertEquals("multiprocess",
                text(child(cluster, "properties"), "rowweave.test.reportSet"));

        Element profileTests = profile(profiles, "profile-tests");
        Element properties = child(profileTests, "properties");
        assertEquals("profile", text(properties, "rowweave.test.reportSet"));
        assertEquals("", text(properties, "rowweave.test.excludedGroups"));
        Element surefire = plugin(child(profileTests, "build"), "maven-surefire-plugin");
        assertEquals("profile", text(child(surefire, "configuration"), "groups"));
    }

    private static Document pom() throws Exception {
        File modulePom = new File(System.getProperty("basedir", "."), "pom.xml");
        return DocumentBuilderFactory.newInstance().newDocumentBuilder().parse(modulePom);
    }

    private static Element profile(Element profiles, String id) {
        for (Element element : children(profiles, "profile")) {
            if (id.equals(text(element, "id"))) {
                return element;
            }
        }
        return null;
    }

    private static Element plugin(Element build, String artifactId) {
        Element plugins = child(build, "plugins");
        for (Element element : children(plugins, "plugin")) {
            if (artifactId.equals(text(element, "artifactId"))) {
                return element;
            }
        }
        return null;
    }

    private static Element child(Element parent, String name) {
        if (parent == null) {
            return null;
        }
        NodeList nodes = parent.getChildNodes();
        for (int i = 0; i < nodes.getLength(); i++) {
            Node node = nodes.item(i);
            if (node instanceof Element && name.equals(node.getNodeName())) {
                return (Element) node;
            }
        }
        return null;
    }

    private static java.util.List<Element> children(Element parent, String name) {
        java.util.List<Element> result = new java.util.ArrayList<>();
        if (parent == null) {
            return result;
        }
        NodeList nodes = parent.getChildNodes();
        for (int i = 0; i < nodes.getLength(); i++) {
            Node node = nodes.item(i);
            if (node instanceof Element && name.equals(node.getNodeName())) {
                result.add((Element) node);
            }
        }
        return result;
    }

    private static String text(Element parent, String name) {
        Element element = child(parent, name);
        return element == null ? null : element.getTextContent().trim();
    }
}
