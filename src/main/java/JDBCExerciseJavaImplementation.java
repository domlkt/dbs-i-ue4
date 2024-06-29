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
import java.util.*;


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

		/*
		var myMovie = new Movie("??????????", "My Movie", 2023, Set.of("Indie"));
		myMovie.actorNames.add("Myself");
		movies.add(myMovie);
		*/


        Statement statement = connection.createStatement();

        //gebraucht werden tconst, title, year, genres (das sind die Attribute des Objektes Movie)
        String anfrage1 = "SELECT tmovies.tconst, tmovies.\"primaryTitle\", tmovies.\"startYear\", tmovies.genres " +
                "FROM tmovies WHERE \"primaryTitle\" LIKE '%" +  keywords + "%' " +
                "ORDER BY \"primaryTitle\" ASC, \"startYear \" ASC";


        ResultSet resultSet = statement.executeQuery(anfrage1);

        while (resultSet.next()) {

            /*start of converting genres to set for movie object*/
            String genres = resultSet.getString("genres").substring(1, resultSet.getString("genres").length() - 1);
            String[] genresArr = genres.split(",");

            Set<String> genresSet = new HashSet<String>();

            for(int i = 0; i < genresArr.length; i++){
                genresSet.add(genresArr[i]);
            }
            /*end*/

            var myMovie = new Movie(resultSet.getString("tconst"), resultSet.getString("primaryTitle"), resultSet.getInt("startYear"), genresSet);
            //myMovie.actorNames.add("Myself");


            //Abfragen, welche Schauspieler in dem Film mitgespielt haben

            String anfrage2 = "SELECT DISTINCT nbasics.primaryname\n" +
                    "FROM nbasics, tprincipals, tmovies" +
                    "WHERE nbasics.nconst = tprincipals.nconst " +
                    "AND tprincipals.tconst = tmovies.tconst AND (tprincipals.category = 'actress' OR tprincipals.category = 'actor') " +
                    "AND tmovies.\"primaryTitle\" =" + resultSet.getString("primaryTitle") +
                    "ORDER BY nbasics.primaryname ASC";


            ResultSet subResultSet = statement.executeQuery(anfrage2);


            while (subResultSet.next()) {
                myMovie.actorNames.add(resultSet.getString("primaryname"));
            }


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

        //throw new UnsupportedOperationException("Not yet implemented");

        return movies;
    }

    @Override
    public List<Actor> queryActors(
            @NotNull Connection connection,
            @NotNull String keywords
    ) throws SQLException {
        logger.info(keywords);
        List<Actor> actors = new ArrayList<>();

//****************************************************************************************************************************************************************


        Statement statement = connection.createStatement();

        //gebraucht werden nconst und name des Schauspielers
        String anfrage1 = "SELECT nbasics.nconst, nbasics.primaryname" +
                "FROM nbasics, tprincipals" +
                "WHERE nbasics.nconst = tprincipals.nconst AND nbasics.primaryname LIKE '%" + keywords + "%'" +
                "AND (tprincipals.category = 'actor' OR tprincipals.category = 'actress')" +
                "GROUP BY nbasics.primaryname" +
                "ORDER BY COUNT(*) DESC, nbasics.primaryname ASC" +
                "LIMIT 5";


        ResultSet resultSet = statement.executeQuery(anfrage1);

        while (resultSet.next()) {

            var myActor = new Actor(resultSet.getString("nconst"), resultSet.getString("primaryname"));


            //Start Subquery 1

            //Abfragen, welche die fünf neuesten Filme sind, in denen der aktuelle Schauspieler mitgespielt hat
            String anfrage2 = "SELECT tmovies.\"primaryTitle\"" +
                    "FROM tmovies, tprincipals, nbasics" +
                    "WHERE tmovies.tconst = tprincipals.tconst AND tprincipals.nconst = nbasics.nconst AND nbasics.primaryname = '" + resultSet.getString("primaryname") + "'" +
                    "ORDER BY tmovies.\"startYear\" DESC, tmovies.\"primaryTitle\" ASC" +
                    "LIMIT 5";


            ResultSet subResultSet = statement.executeQuery(anfrage2);


            while (subResultSet.next()) {
                myActor.playedIn.add(subResultSet.getString("tmovies.\"primaryTitle\""));
            }

            //End Subquery 1

            //Start Subquery 2

            //Abfragen, welche die fünf häufigsten Costars des aktuellen Schauspielers sind
            String anfrage3 = "select nb2.primaryname, count(*)" +
                    "from tprincipals tp1, tprincipals tp2, nbasics nb1, nbasics nb2" +
                    "where tp1.tconst = tp2.tconst and tp1.nconst < tp2.nconst and nb1.nconst = tp1.nconst and nb2.nconst = tp2.nconst" +
                    "and nb1.primaryname = 'Rupert Grint' and (tp2.category = 'actress' or tp2.category = 'actor')" +
                    "group by nb1.primaryname, nb2.primaryname" +
                    "order by count(*) desc, nb2.primaryname asc limit 5";


            ResultSet subResultSet2 = statement.executeQuery(anfrage3);


            while (subResultSet2.next()) {
                myActor.costarNameToCount.put(subResultSet2.getString("nb2.primaryname"), Integer.parseInt(subResultSet2.getString("count(*)")));
            }

            //End Subquery 2





            actors.add(myActor);

        }
//****************************************************************************************************************************************************************

		/*
		SQL Anfrage 1, Top 5 Schauspieler in 'keyword' Filmen:
		----------------------------------
		WITH topfiveactors AS
		(
			SELECT nbasics.primaryname
			FROM nbasics, tprincipals
			WHERE nbasics.nconst = tprincipals.nconst AND nbasics.primaryname LIKE '%" + keywords + "%'
	        AND (tprincipals.category = 'actor' OR tprincipals.category = 'actress')
			GROUP BY nbasics.primaryname
			ORDER BY COUNT(*) DESC, nbasics.primaryname ASC
			LIMIT 5
		),
		bestmoviesfrommactor AS
		(
			SELECT tmovies."primaryTitle"
			FROM tmovies, tprincipals, nbasics
			WHERE tmovies.tconst = tprincipals.tconst AND tprincipals.nconst = nbasics.nconst AND nbasics.primaryname = '" + schauspielername + "'
			ORDER BY tmovies."startYear" DESC, tmovies."primaryTitle" ASC
			LIMIT 5
		),
		costars AS
		(
		select tp1.nconst, tp2.nconst, count(*)
	    from tprincipals tp1, tprincipals tp2
	    where tp1.tconst = tp2.tconst and tp1.nconst < tp2.nconst
	    group by tp1.nconst, tp2.nconst
		)





		SELECT topfiveactors.name,

		*/

        //Für letzte Anfrage self join über nbasics (für Namen) und tprincipals und dann count wer am meisten mit actor 1 gearbeitet hat

        //throw new UnsupportedOperationException("Not yet implemented");

        return actors;
    }
}
