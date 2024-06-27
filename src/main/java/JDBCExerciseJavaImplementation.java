import de.hpi.dbs1.ChosenImplementation;
import de.hpi.dbs1.ConnectionConfig;
import de.hpi.dbs1.JDBCExercise;
import de.hpi.dbs1.entities.Actor;
import de.hpi.dbs1.entities.Movie;
import org.jetbrains.annotations.NotNull;

import java.sql.Connection;
import java.sql.SQLException;
import java.util.ArrayList;
import java.util.List;
import java.util.Set;
import java.util.logging.Logger;

import java.sql.*; //hinzugefügt

@ChosenImplementation(true)
public class JDBCExerciseJavaImplementation implements JDBCExercise {

    Logger logger = Logger.getLogger(this.getClass().getSimpleName());

    //passes test
    @Override
    public Connection createConnection(@NotNull ConnectionConfig config) throws SQLException {

        String url = "jdbc:postgresql://localhost:5432/imdb";
        String username = config.getUsername();
        String password = config.getPassword();


        // Load the PostgreSQL JDBC driver (optional step for newer JDBC versions)
        try {
            Class.forName("org.postgresql.Driver");
        } catch (ClassNotFoundException e) {
            throw new SQLException("PostgreSQL JDBC driver not found.", e);
        }

        // Create connection
        Connection db = DriverManager.getConnection(url, username, password);

        return db;
    }

    @Override
    public List<Movie> queryMovies(@NotNull Connection connection, @NotNull String keywords) throws SQLException {
        logger.info(keywords);
        List<Movie> movies = new ArrayList<>();

        //Verbindung zu DB herstellen
        //schauen, wie man SQL Befehle abschickt und wie man die Ergebnisse empfängt


		/*
		var myMovie = new Movie("??????????", "My Movie", 2023, Set.of("Indie"));
		myMovie.actorNames.add("Myself");
		movies.add(myMovie);
		*/


        // Create a statement object to execute queries
        Statement statement = connection.createStatement();


        /*
		Skizze SQL Anfrage 1, Filme mit String "keyword" im Namen:
		------------
		SELECT primaryTitle FROM tmovies
		WHERE titel LIKE '% + keyword + %'
		ORDER BY titel ASC, year ASC
		*/

        //gebraucht werden tconst, title, year, genres (das sind die Attribute des Objektes Movie)
        String anfrage1 = "SELECT \"primaryTitle\" FROM tmovies WHERE \"primaryTitle\" LIKE '%" +  keywords + "%' ORDER BY \"primaryTitle\" ASC, \"startYear \" ASC";


        // Execute a query and get the result set
        ResultSet resultSet = statement.executeQuery(anfrage1);

        //das ergebnis jetzt in liste zwischenspeichern

        //wir haben nur Filmnamen abgefragt, deswegen werden auch nur diese in Liste geschrieben werden können
        while (resultSet.next()) {
            //only colum is primarytitle of respective movies
            System.out.println("Column1: " + resultSet.getString("primaryTitle"));

            var myMovie = new Movie("??????????", resultSet.getString("primaryTitle"), -1, Set.of(""));
            //myMovie.actorNames.add("Myself");
            movies.add(myMovie);
        }



		/*
		Skizze SQL Anfrage 2, Schauspielerinnen in dem Film:
		------------
		SELECT nbasics."primaryName"
		FROM nbasics, tprincipals, tmovies
		WHERE nbasics.nconst = tprincipals.nconst AND tprincipals.tconst = tmovies.tconst AND tprincipals.category = 'actress'
		AND tmovies."primaryTitle" IN
		(
			SELECT primaryTitle FROM tmovies
			WHERE titel LIKE '% + keyword + %'
			ORDER BY titel ASC, year ASC
		)
		ORDER BY nbasics."primaryName" ASC
		*/

        String anfrage2 = "SELECT DISTINCT nbasics.primaryname\n" +
                "FROM nbasics, tprincipals, tmovies" +
                "WHERE nbasics.nconst = tprincipals.nconst AND tprincipals.tconst = tmovies.tconst AND tprincipals.category = 'actress'" +
                "AND tmovies.\"primaryTitle\" IN" +
                "(" +
                "SELECT \"primaryTitle\" FROM tmovies" +
                "WHERE \"primaryTitle\" LIKE '%" + keywords + "%'" +
                "ORDER BY \"primaryTitle\" ASC, \"startYear\" ASC" +
                ")" +
                "ORDER BY nbasics.primaryname ASC";

        throw new UnsupportedOperationException("Not yet implemented");
    }

    @Override
    public List<Actor> queryActors(
            @NotNull Connection connection,
            @NotNull String keywords
    ) throws SQLException {
        logger.info(keywords);
        List<Actor> actors = new ArrayList<>();


		/*
		SQL Anfrage 1, Top 5 Schauspieler in 'keyword' Filmen:
		----------------------------------
		WITH topfiveactors AS
		(
			SELECT nbasics."primaryName" AS name
			FROM nbasics, tprincipals
			WHERE nbasics.nconst = tprincipals.nconst AND nbasics."primaryName" LIKE '% + keyword + %'  AND tprincipals.category = 'actor' OR tprincipals.category = 'actress'
			GROUP BY nbasics."primaryName"
			ORDER BY COUNT(*) ASC, nbasics."primaryName" ASC
			LIMIT 5
		),
		bestmoviesfrommactor AS
		(
			SELECT tmovies."primaryTitle"
			FROM tmovies, tprincipals, nbasics
			WHERE tmovies.tconst = tprincipals.tconst AND tprincipals.nconst = nbasics.nconst AND nbasics.name = 'HIER SCHAUSPIELERNAMEN EINFÜGEN'
			ORDER BY tmovies."startYear" DESC, tmovies."primaryTitle" ASC
			LIMIT 5
		)
		SELECT topfiveactors.name,

		*/

        //Für letzte Anfrage self join über nbasics (für Namen) und tprincipals und dann count wer am meisten mit actor 1 gearbeitet hat

        throw new UnsupportedOperationException("Not yet implemented");
    }
}
