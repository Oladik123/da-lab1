import org.apache.commons.compress.compressors.CompressorException;
import org.apache.commons.compress.compressors.CompressorStreamFactory;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;

import javax.xml.stream.XMLStreamException;
import javax.xml.stream.XMLStreamReader;
import javax.xml.stream.events.XMLEvent;
import java.io.IOException;
import java.net.URL;
import java.nio.file.Files;
import java.nio.file.Path;
import java.util.*;
import java.util.stream.Collectors;

import static java.lang.String.format;
import static org.apache.commons.compress.compressors.CompressorStreamFactory.BZIP2;

public class OSMStatsCollector {
    private static final Logger logger = LogManager.getLogger(OSMStatsCollector.class.getName());
    private final Map<String, Long> userChangesMap;
    private final Map<String, Long> uidMarksMap;
    private long amountOfNodes = 0;

    public OSMStatsCollector() {
        userChangesMap = new HashMap<>();
        uidMarksMap = new HashMap<>();
    }

    public void collectStats() {
        var resource = getClass().getClassLoader().getResource("RU-NVS.osm.bz2");

        try (var processor = new StaxStreamWrapper(new CompressorStreamFactory()
                .createCompressorInputStream(BZIP2, Files.newInputStream(Path.of(Objects.requireNonNull(resource).getPath()))))) {
            XMLStreamReader reader = processor.getReader();
            while (reader.hasNext()) {       // while not end of XML
                var event = reader.next();
                if (event != XMLEvent.START_ELEMENT || !"node".equals(reader.getLocalName())) {
                    continue;
                }

                amountOfNodes++;
                var userNameIndex = -1;
                var uidIndex = -1;

                for (var i = 0; i < reader.getAttributeCount(); i++) {
                    final var attributeName = reader.getAttributeLocalName(i);
                    if ("user".equals(attributeName)) {
                        userNameIndex = i;
                    }

                    if ("uid".equals(attributeName)) {
                        uidIndex = i;
                    }
                }

                if (userNameIndex != -1) {
                    addChange(reader.getAttributeValue(userNameIndex));
                } else {
                    logger.error("in node property 'user' not found");
                }

                if (uidIndex != -1) {
                    addMark(reader.getAttributeValue(uidIndex));
                } else {
                    logger.error("in node property 'uid' not found");
                }
            }


        } catch (XMLStreamException | IOException | CompressorException e) {
            logger.error("error on processing stats: " + e.getMessage());
        }

        logger.info("processing stats was ended");


        List<UserChanges> sortedUserChangesList = userChangesMap.entrySet().parallelStream()
                .sorted(Collections.reverseOrder(Map.Entry.comparingByValue()))
                .map((entry) -> new UserChanges(entry.getKey(), entry.getValue()))
                .collect(Collectors.toList());

        List<UserMarks> sortedUidMarksList = uidMarksMap.entrySet().parallelStream()
                .sorted(Collections.reverseOrder(Map.Entry.comparingByValue()))
                .map((entry) -> new UserMarks(entry.getKey(), entry.getValue()))
                .collect(Collectors.toList());


        logger.info(format("list with user and his amount of changes size is %s", sortedUserChangesList.size()));
        logger.info(format("amount of nodes wit user changes info is %s", amountOfNodes));


        var userChangesOutput = new StringBuilder("first\n");
        sortedUserChangesList.stream().limit(50).forEach(userChanges -> {
            logger.info((format("User %s amount of changes is %s", userChanges.userId, userChanges.amountOfChanges)));
            userChangesOutput
                    .append(format("User %s amount of changes is %s", userChanges.userId, userChanges.amountOfChanges))
                    .append("\n");

        });

        var uidMarksOutput = new StringBuilder("second\n");
        sortedUidMarksList.stream().limit(50).forEach(uidMarks -> {
            logger.info((format("Uid %s amount of marks is %s", uidMarks.userId, uidMarks.amountOfMarks)));
            uidMarksOutput
                    .append((format("Uid %s amount of marks is %s", uidMarks.userId, uidMarks.amountOfMarks)))
                    .append("\n");
        });

        System.out.println(userChangesOutput);
        System.out.println(uidMarksOutput);
    }

    private void addChange(String user) {
        if (userChangesMap.containsKey(user)) {
            long userChanges = userChangesMap.get(user);
            logger.debug(format("User with name %s already in map, his changes amount is %s", user, userChanges));
            userChangesMap.put(user, ++userChanges);
        } else {
            logger.debug(format("User with name %s found first time, his changes amount is initial", user));
            userChangesMap.put(user, 0L);
        }
    }

    private void addMark(String user) {
        if (uidMarksMap.containsKey(user)) {
            long marks = uidMarksMap.get(user);
            logger.debug(format("Uid %s already in map, his nodes marks amount is %s", user, marks));
            uidMarksMap.put(user, ++marks);
        } else {
            logger.debug(format("Uid %s found first time, his nodes marks is initial", user));
            uidMarksMap.put(user, 0L);
        }
    }
}
