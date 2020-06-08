import model.UserChanges;
import model.UserMarks;
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
import java.util.*;
import java.util.stream.Collectors;

import static java.lang.String.format;
import static org.apache.commons.compress.compressors.CompressorStreamFactory.BZIP2;

public class OSMStatsCollector {
    private static final Logger logger = LogManager.getLogger(OSMStatsCollector.class.getName());
    private Map<String, Long> userChangesMap;
    private Map<Integer, Long> uidMarksMap;
    private long amountOfNodes = 0;
    private JAXBContext jaxbContext = JAXBContext.newInstance(Node.class);
    private Unmarshaller unmarshaller = jaxbContext.createUnmarshaller();

    public OSMStatsCollector() throws JAXBException {
        userChangesMap = new HashMap<>();
        uidMarksMap = new HashMap<>();
    }

    public void processStats() {
        URL resource = getClass().getClassLoader().getResource("RU-NVS.osm.bz2");
        try (StaxStreamWrapper processor = new StaxStreamWrapper(new CompressorStreamFactory()
                .createCompressorInputStream(BZIP2, Files.newInputStream(Paths.get(Objects.requireNonNull(resource).getPath()))))) {
            XMLStreamReader reader = processor.getReader();
            while (reader.hasNext()) {
                int event = reader.next();
                if (event == XMLEvent.START_ELEMENT && "node".equals(reader.getLocalName())) {
                    amountOfNodes++;
                    Node node = (Node) unmarshaller.unmarshal(reader);
                    userChangesMap.compute(node.getUser(), (k, v) -> (v == null) ? 1 : 1 + v);
                    uidMarksMap.compute(node.getUid().intValue(), (k, v) -> (v == null) ? 1 : 1 + v);
                }
            }

            logger.info("processing stats was ended");


            List<UserChanges> sortedUserChangesList = userChangesMap.entrySet().stream()
                    .sorted(Collections.reverseOrder(Map.Entry.comparingByValue()))
                    .map((entry) -> new UserChanges(entry.getKey(), entry.getValue()))
                    .collect(Collectors.toList());

            List<UserMarks> sortedUidMarksList = uidMarksMap.entrySet().stream()
                    .sorted(Collections.reverseOrder(Map.Entry.comparingByValue()))
                    .map((entry) -> new UserMarks(entry.getKey().toString(), entry.getValue()))
                    .collect(Collectors.toList());


            logger.info(format("list with user and his amount of changes size is %s", sortedUserChangesList.size()));
            logger.info(format("amount of nodes wit user changes info is %s", amountOfNodes));


            StringBuilder userChangesOutput = new StringBuilder("first\n");
            sortedUserChangesList.stream().limit(50).forEach(userChanges -> {
                logger.info((format("User %s amount of changes is %s", userChanges.userId, userChanges.amountOfChanges)));
                userChangesOutput
                        .append(format("User %s amount of changes is %s", userChanges.userId, userChanges.amountOfChanges))
                        .append("\n");

            });

            StringBuilder uidMarksOutput = new StringBuilder("second\n");
            sortedUidMarksList.stream().limit(50).forEach(uidMarks -> {
                logger.info((format("Uid %s amount of marks is %s", uidMarks.userId, uidMarks.amountOfMarks)));
                uidMarksOutput
                        .append((format("Uid %s amount of marks is %s", uidMarks.userId, uidMarks.amountOfMarks)))
                        .append("\n");
            });

            System.out.println(userChangesOutput);
            System.out.println(uidMarksOutput);

        } catch (XMLStreamException | IOException | JAXBException | CompressorException e) {
            logger.error("error on processing xml, {}", e.getLocalizedMessage());
            e.printStackTrace();
        }
    }
}
