package dao;

import config.ConnectionFactory;
import model.osm.Node;

import javax.xml.datatype.DatatypeFactory;
import javax.xml.datatype.XMLGregorianCalendar;
import java.math.BigDecimal;
import java.math.BigInteger;
import java.sql.*;
import java.time.LocalDateTime;
import java.util.List;
import java.util.Optional;

public class NodeDao {

    public boolean insertNodeUsingPreparedStatement(Node node) {
        try (Connection connection = ConnectionFactory.getConnection()) {
            PreparedStatement ps = connection.prepareStatement("INSERT INTO nodes VALUES (default,?, ?, ?, ?, ?, ?, ?, ?)", Statement.RETURN_GENERATED_KEYS);
            convertNodeToPS(node, ps);

            int i = ps.executeUpdate();
            connection.close();

            return i == 1;
        } catch (SQLException ex) {
            ex.printStackTrace();
            return false;
        }

    }

    public boolean insertNodeUsingStatement(Node node) {
        try (Connection connection = ConnectionFactory.getConnection()) {
            Statement statement = connection.createStatement();

            Timestamp timestamp = new Timestamp(node.getTimestamp().toGregorianCalendar().getTimeInMillis());
            String query = String.format("INSERT INTO nodes VALUES (default, %d, %d, '%tF', %d, '%s', %d, %f, %f)",
                    node.getId(),
                    node.getVersion(),
                    timestamp,
                    node.getUid(),
                    node.getUser(),
                    node.getChangeset(),
                    node.getLat(),
                    node.getLon()
            );
            int i = statement.executeUpdate(query);
            return i == 1;
        } catch (SQLException e) {
            e.printStackTrace();
            return false;
        }

    }

    public void insertBatch(List<Node> nodes) {
        try (Connection connection = ConnectionFactory.getConnection()) {
            PreparedStatement ps = connection.prepareStatement("INSERT INTO nodes VALUES (default,?, ?, ?, ?, ?, ?, ?, ?)");
            int count = 0;

            for (Node node : nodes) {
                convertNodeToPS(node, ps);
                ps.addBatch();
                count++;

                if (count % 100 == 0 || count == nodes.size()) {
                    ps.executeBatch();
                    count = 0;
                }

            }

        } catch (SQLException ex) {
            ex.printStackTrace();
        }
    }

    public Optional<Node> getNodeById(Integer id) {
        try (Connection connection = ConnectionFactory.getConnection()) {
            Statement stmt = connection.createStatement();
            ResultSet rs = stmt.executeQuery("SELECT * FROM nodes WHERE id=" + id);
            if (rs.next()) {
                return Optional.ofNullable(extractNodeFromResultSet(rs));
            } else {
                return Optional.empty();
            }
        } catch (Exception ex) {
            ex.printStackTrace();
            return Optional.empty();
        }
    }

    public boolean updateNode(Node node) {
        try (Connection connection = ConnectionFactory.getConnection()) {
            PreparedStatement ps = connection.prepareStatement(
                    "UPDATE nodes SET id = ?, version = ?, timestamp = ?, uid = ?, user = ?, changeset = ?, lat = ?, lon = ? WHERE id = ?");

            convertNodeToPS(node, ps);
            int i = ps.executeUpdate();
            return i == 1;
        } catch (SQLException ex) {
            ex.printStackTrace();
            return false;
        }
    }

    public boolean deleteNode(Integer id) {
        try (Connection connection = ConnectionFactory.getConnection()) {
            Statement stmt = connection.createStatement();
            int i = stmt.executeUpdate("DELETE FROM nodes WHERE id=" + id);
            return i == 1;

        } catch (SQLException ex) {
            ex.printStackTrace();
            return false;
        }
    }

    private void convertNodeToPS(Node node, PreparedStatement ps) throws SQLException {
        ps.setLong(1, node.getId().longValue());
        ps.setLong(2, node.getVersion().longValue());
        ps.setTimestamp(3, new Timestamp(node.getTimestamp().toGregorianCalendar().getTimeInMillis()));
        ps.setLong(4, node.getUid().longValue());
        ps.setString(5, node.getUser());
        ps.setLong(6, node.getChangeset().longValue());
        ps.setDouble(7, node.getLat());
        ps.setDouble(8, node.getLon());
    }

    private Node extractNodeFromResultSet(ResultSet rs) throws Exception {
        Node node = new Node();
        node.setId(BigInteger.valueOf(rs.getLong("id")));
        node.setVersion(BigInteger.valueOf(rs.getLong("version")));
        XMLGregorianCalendar xmlGregorianCalendar = convertLocalDataTimeToXmlGCal(rs.getTimestamp("timestamp"));
        node.setTimestamp(xmlGregorianCalendar);
        node.setUid(BigInteger.valueOf(rs.getLong("uid")));
        node.setUser(rs.getString("user"));
        node.setChangeset(BigInteger.valueOf(rs.getLong("changeset")));
        node.setLat(rs.getDouble("lat"));
        node.setLon(rs.getDouble("lon"));
        return node;
    }

    private XMLGregorianCalendar convertLocalDataTimeToXmlGCal(Timestamp timestamp) throws Exception {
        LocalDateTime ldt = timestamp.toLocalDateTime();
        XMLGregorianCalendar cal = DatatypeFactory.newInstance().newXMLGregorianCalendar();
        cal.setYear(ldt.getYear());
        cal.setMonth(ldt.getMonthValue());
        cal.setDay(ldt.getDayOfMonth());
        cal.setHour(ldt.getHour());
        cal.setMinute(ldt.getMinute());
        cal.setSecond(ldt.getSecond());
        cal.setFractionalSecond(new BigDecimal("0." + ldt.getNano()));
        return cal;
    }
}
