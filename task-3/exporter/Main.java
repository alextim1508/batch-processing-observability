import java.sql.*;
import java.io.*;
import java.nio.file.*;
import java.time.ZoneId;
import java.time.ZonedDateTime;
import java.time.format.DateTimeFormatter;
import java.util.Properties;

public class Main {

    private static String env(String name, String defaultValue, boolean required) {
        String value = System.getenv(name);
        if (required && (value == null || value.isEmpty())) {
            System.err.println("can't find property: " + name);
            System.exit(42);
        }
        return value != null ? value : defaultValue;
    }

    public static void main(String[] args) throws SQLException, IOException {
        String pgHost = env("PGHOST", null, true);
        int pgPort = Integer.parseInt(env("PGPORT", "5432", false));
        String pgDatabase = env("PGDATABASE", null, true);
        String pgUser = env("PGUSER", null, true);
        String pgPassword = env("PGPASSWORD", null, true);
        String table = env("TABLE", "shipments", false);
        Path outputDir = Paths.get(env("OUTPUT_DIR", "/data", false));

        Files.createDirectories(outputDir);

        String timestamp = ZonedDateTime.now(ZoneId.of("UTC"))
                .format(DateTimeFormatter.ofPattern("yyyyMMdd"));
        Path outputFile = outputDir.resolve(String.format("%s-%s.csv", table, timestamp));

        String connUrl = String.format("jdbc:postgresql://%s:%d/%s", pgHost, pgPort, pgDatabase);

        Properties props = new Properties();
        props.setProperty("user", pgUser);
        props.setProperty("password", pgPassword);

        System.out.printf("table=%s to file: %s%n", table, outputFile);

        try (Connection conn = DriverManager.getConnection(connUrl, props);
             Statement stmt = conn.createStatement();
             ResultSet rs = stmt.executeQuery("SELECT * FROM " + table)) {

            try (PrintWriter pw = new PrintWriter(new FileWriter(outputFile.toFile()))) {
                ResultSetMetaData metaData = rs.getMetaData();
                int columnCount = metaData.getColumnCount();

                for (int i = 1; i <= columnCount; i++) {
                    pw.print(metaData.getColumnName(i));
                    if (i < columnCount) pw.print(",");
                }
                pw.println();

                while (rs.next()) {
                    for (int i = 1; i <= columnCount; i++) {
                        pw.print(rs.getString(i));
                        if (i < columnCount) pw.print(",");
                    }
                    pw.println();
                }
            }
        }

        try {
            System.out.println(Files.readString(outputFile));
        } catch (IOException e) {
            System.out.println("error while printing file: " + e.getMessage());
        }

        long size = Files.size(outputFile);
        System.out.printf("file=%s, size=%d%n", outputFile, size);
    }
}