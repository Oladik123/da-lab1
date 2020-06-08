import com.google.common.base.Stopwatch;
import config.DatabaseConfig;
import dao.NodeDao;
import model.osm.Node;
import org.apache.commons.compress.compressors.CompressorException;
import org.apache.commons.compress.compressors.CompressorStreamFactory;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;

import javax.xml.bind.JAXBContext;
import javax.xml.bind.JAXBException;
import javax.xml.bind.Unmarshaller;
import javax.xml.stream.XMLStreamException;
import javax.xml.stream.XMLStreamReader;
import javax.xml.stream.events.XMLEvent;
import java.io.IOException;
import java.net.URL;
import java.nio.file.Files;
import java.nio.file.Paths;
import java.util.ArrayList;
import java.util.List;
import java.util.Objects;

import static org.apache.commons.compress.compressors.CompressorStreamFactory.BZIP2;

public class NodeLoader {
    private static final Logger logger = LogManager.getLogger(NodeLoader.class.getName());
    private final NodeDao nodeDao;
    private final JAXBContext jaxbContext = JAXBContext.newInstance(Node.class);
    private final Unmarshaller unmarshaller = jaxbContext.createUnmarshaller();
    private long amountOfNodes = 0;

    public NodeLoader() throws JAXBException {
        this.nodeDao = new NodeDao();
    }

    public void loadNodesWithDifferentWays() {
        loadNodesUsingPreparedStatement();
        loadNodesUsingStatement();
        loadNodesUsingStatementWithBatch();
    }


    private void loadNodesUsingPreparedStatement() {
        amountOfNodes = 0;
        DatabaseConfig.dropTables();
        DatabaseConfig.initTables();
        Stopwatch timer = Stopwatch.createStarted();
        URL resource = getClass().getClassLoader().getResource("RU-NVS.osm.bz2");
        try (StaxStreamWrapper processor = new StaxStreamWrapper(new CompressorStreamFactory()
                .createCompressorInputStream(BZIP2, Files.newInputStream(Paths.get(Objects.requireNonNull(resource).getPath()))))) {
            XMLStreamReader reader = processor.getReader();
            while (reader.hasNext()) {
                int event = reader.next();
                if (event == XMLEvent.START_ELEMENT && "node".equals(reader.getLocalName())) {
                    amountOfNodes++;
                    Node node = (Node) unmarshaller.unmarshal(reader);
                    nodeDao.insertNodeUsingPreparedStatement(node);
                }
            }
        } catch (XMLStreamException | IOException | JAXBException | CompressorException e) {
            logger.info("error on loading nodes");
            e.printStackTrace();
        }
        logger.info("amountOfNodes: " + amountOfNodes);
        logger.info("Method loadNodesUsingPreparedStatement took: " + timer.stop());
    }

    private void loadNodesUsingStatement() {
        amountOfNodes = 0;
        DatabaseConfig.dropTables();
        DatabaseConfig.initTables();
        Stopwatch timer = Stopwatch.createStarted();
        URL resource = getClass().getClassLoader().getResource("RU-NVS.osm.bz2");
        try (StaxStreamWrapper processor = new StaxStreamWrapper(new CompressorStreamFactory()
                .createCompressorInputStream(BZIP2, Files.newInputStream(Paths.get(Objects.requireNonNull(resource).getPath()))))) {
            XMLStreamReader reader = processor.getReader();
            while (reader.hasNext()) {
                int event = reader.next();
                if (event == XMLEvent.START_ELEMENT && "node".equals(reader.getLocalName())) {
                    amountOfNodes++;
                    Node node = (Node) unmarshaller.unmarshal(reader);
                    nodeDao.insertNodeUsingStatement(node);
                }
            }
        } catch (XMLStreamException | IOException | JAXBException | CompressorException e) {
            logger.info("error on loading nodes");
            e.printStackTrace();
        }

        logger.info("amountOfNodes: " + amountOfNodes);
        logger.info("Method loadNodesUsingStatement took: " + timer.stop());
    }

    private void loadNodesUsingStatementWithBatch() {
        amountOfNodes = 0;
        DatabaseConfig.dropTables();
        DatabaseConfig.initTables();
        Stopwatch timer = Stopwatch.createStarted();
        URL resource = getClass().getClassLoader().getResource("RU-NVS.osm.bz2");
        try (StaxStreamWrapper processor = new StaxStreamWrapper(new CompressorStreamFactory()
                .createCompressorInputStream(BZIP2, Files.newInputStream(Paths.get(Objects.requireNonNull(resource).getPath()))))) {
            XMLStreamReader reader = processor.getReader();
            int count = 0;
            List<Node> nodes = new ArrayList<>();
            while (reader.hasNext()) {
                int event = reader.next();
                if (event == XMLEvent.START_ELEMENT && "node".equals(reader.getLocalName())) {
                    amountOfNodes++;
                    Node node = (Node) unmarshaller.unmarshal(reader);
                    nodes.add(node);
                    count++;

                    if (count % 100 == 0) {
                        nodeDao.insertBatch(nodes);
                        count = 0;
                        nodes = new ArrayList<>();
                    }
                }
            }
            nodeDao.insertBatch(nodes);
        } catch (XMLStreamException | IOException | JAXBException | CompressorException e) {
            logger.info("error on loading nodes");
            e.printStackTrace();
        }

        logger.info("amountOfNodes: " + amountOfNodes);
        logger.info("Method loadNodesUsingStatementWithBatch took: " + timer.stop());
    }

}
