import java.sql.Connection;
import java.sql.PreparedStatement;
import java.sql.SQLException;
import java.sql.Timestamp;
import java.time.Instant;
import java.time.Duration;
import java.util.ArrayList;
import java.util.List;
import java.util.logging.Level;
import java.util.logging.Logger;

public class DbTool {
    private static final Logger LOGGER = Logger.getLogger(DbTool.class.getName());
    private static final DbUtil dbUtil = new DbUtil();
    private static final String SQL_LAGERORT_INSERT_UPDATE =
            "INSERT INTO lagerort (id, cdate, mdate, version, status, lstnr, art_id, regal, fach, bereich, haupt_lagerort) " +
                    "VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?) " +
                    "ON DUPLICATE KEY UPDATE " +
                    "cdate = VALUES(cdate), mdate = VALUES(mdate), version = VALUES(version), " +
                    "status = VALUES(status), lstnr = VALUES(lstnr), art_id = VALUES(art_id), " +
                    "regal = VALUES(regal), fach = VALUES(fach), bereich = VALUES(bereich), haupt_lagerort = VALUES(haupt_lagerort)";

    public static void main(String[] args) {
        List<ArtikelLagerort> lagerorte = generiereLagerorte(10);
        int result = insertLagerorteBulk(lagerorte);
        LOGGER.info("Anzahl der aktualisierten Lagerorte: " + result);
    }

    private static List<ArtikelLagerort> generiereLagerorte(int anzahl) {
        if (anzahl < 0) {
            throw new IllegalArgumentException("Anzahl muss positiv sein");
        }
        List<ArtikelLagerort> lagerorte = new ArrayList<>();
        for (int i = 1; i <= anzahl; i++) {
            lagerorte.add(new ArtikelLagerort(i, i));
        }
        return lagerorte;
    }

    private static int insertLagerorteBulk(List<ArtikelLagerort> lagerorte) {
        int result = 0;
        Instant start = Instant.now();

        try (Connection connection = dbUtil.getConnection();
             PreparedStatement preparedStatement = connection.prepareStatement(SQL_LAGERORT_INSERT_UPDATE)) {
            connection.setAutoCommit(false);

            for (ArtikelLagerort lagerort : lagerorte) {
                int index = 1;
                preparedStatement.setString(index++, lagerort.getId());
                preparedStatement.setTimestamp(index++, Timestamp.valueOf(lagerort.getCdate()));
                preparedStatement.setTimestamp(index++, Timestamp.valueOf(lagerort.getMdate()));
                preparedStatement.setInt(index++, lagerort.getVersion());
                preparedStatement.setInt(index++, lagerort.getStatus());
                preparedStatement.setInt(index++, lagerort.getLstnr());
                preparedStatement.setInt(index++, lagerort.getArtId());
                preparedStatement.setString(index++, lagerort.getRegal());
                preparedStatement.setString(index++, lagerort.getFach());
                preparedStatement.setString(index++, lagerort.getBereich());
                preparedStatement.setInt(index++, lagerort.getHauptLagerort());
                preparedStatement.addBatch();
            }

            preparedStatement.executeBatch();
            connection.commit();
            result = lagerorte.size();
        } catch (SQLException e) {
            LOGGER.log(Level.SEVERE, "Fehler beim Ausführen der Batch-Operation: " + e.getMessage(), e);
            if (connection != null) {
                try {
                    connection.rollback();
                } catch (SQLException exception) {
                    LOGGER.log(Level.SEVERE, "Fehler beim Rollback: " + exception.getMessage(), exception);
                }
            }
        }

        Instant end = Instant.now();
        long timeElapsed = Duration.between(start, end).toMillis();
        LOGGER.info("Zeit für die Durchführung der Inserts: " + timeElapsed + " ms");

        return result;
    }
}
