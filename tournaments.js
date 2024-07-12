const { Client } = require('pg');

// PostgreSQL connection details
const client = new Client({
  user: 'rrtladmin',
  host: 'rrtl.postgres.database.azure.com',
  database: 'postgres',
  password: 'Zulifqar_me3',
  port: 5432,
  ssl: {
    rejectUnauthorized: false
  }
});

// Function to fetch and process data
async function fetchDataAndPrepareInsertQuery() {
  try {
    // Fetch teams data from DynamoDB
    const teamsDynamo = [
      {
        "id": 8,
        "championshipName": "Spring 2024 League",
        "player_of_the_tournament": 'sameer-sharma',
        "best_team_of_tournament": 'Brown Mambas',
        "start_date": "04-1-2024",
        "end_date": "05-20-2024",
        "winnerTeamName": "Gladiators",
        "runnerUpTeamName": "Mavericks",
        "winnerPlayerOne": "Badar Usmani",
        "winnerPlayerOneId": "busmani84@gmail.com",
        "winnerPlayerTwo": "Asif Hasan",
        "winnerPlayerTwoId": "asifh316@gmail.com",
        "winnerPointsSetOne": 6,
        "winnerPointsSetTwo": 6,
        "runnerUpPlayerOne": "Sameer Sharma",
        "runnerUpPlayerOneId": "sameer-sharma",
        "runnerUpPlayerTwo": "Majid Atique",
        "runnerUpPlayerTwoId": "majid.atique@gmail.com",
        "runnerUpPointsSetOne": 3,
        "runnerUpPointsSetTwo": 2
      },
      {
        "id": 7,
        "start_date": "07-1-2023",
        "end_date": "08-20-2023",
        "championshipName": "Summer 2023 League",
        "winnerTeamName": "Speedy Gonzales",
        "runnerUpTeamName": "Amigos",
        "winnerPlayerOne": "Umair Rahim",
        "winnerPlayerOneId": "umairrahim@gmail.com",
        "winnerPlayerTwo": "Asim Ahmed",
        "winnerPlayerTwoId": "ahmedas09@gmail.com",
        "winnerPointsSetOne": 4,
        "winnerPointsSetTwo": 6,
        "winnerPointsSetThree": 6,
        "runnerUpPlayerOne": "Zulifqar Ahmad",
        "runnerUpPlayerOneId": "zulifqar.ahmad.malik@gmail.com",
        "runnerUpPlayerTwo": "Talha Khan",
        "runnerUpPlayerTwoId": "talha.khan2k@gmail.com",
        "runnerUpPointsSetOne": 6,
        "runnerUpPointsSetTwo": 1,
        "runnerUpPointsSetThree": 3
      },
      {
        "id": 6,
        "start_date": "03-1-2023",
        "end_date": "04-20-2023",
        "championshipName": "Spring 2023 League",
        "winnerTeamName": "Gladiators",
        "runnerUpTeamName": "Terminator",
        "winnerPlayerOne": "Badar Usmani",
        "winnerPlayerOneId": "busmani84@gmail.com",
        "winnerPlayerTwo": "Asif Hasan",
        "winnerPlayerTwoId": "asifh316@gmail.com",
        "winnerPointsSetOne": 6,
        "winnerPointsSetTwo": 6,
        "runnerUpPlayerOne": "Zulifqar Ahmad",
        "runnerUpPlayerOneId": "zulifqar.ahmad.malik@gmail.com",
        "runnerUpPlayerTwo": "Majid Atique",
        "runnerUpPlayerTwoId": "majid.atique@gmail.com",
        "runnerUpPointsSetOne": 4,
        "runnerUpPointsSetTwo": 4
      },
      {
        "id": 5,
        "start_date": "09-1-2022",
        "end_date": "10-20-2022",
        "championshipName": "Fall 2022 League",
        "winnerTeamName": "Gladiators",
        "runnerUpTeamName": "SmashBros",
        "winnerPlayerOne": "Badar Usmani",
        "winnerPlayerOneId": "busmani84@gmail.com",
        "winnerPlayerTwo": "Asif Hasan",
        "winnerPlayerTwoId": "asifh316@gmail.com",
        "winnerPointsSetOne": 6,
        "winnerPointsSetTwo": 0,
        "runnerUpPlayerOne": "Umair Rahim",
        "runnerUpPlayerOneId": "umairrahim@gmail.com",
        "runnerUpPlayerTwo": "Talha Khan",
        "runnerUpPlayerTwoId": "talha.khan2k@gmail.com",
        "runnerUpPointsSetOne": 0,
        "runnerUpPointsSetTwo": 1
      },
      {
        "id": 4,
        "start_date": "06-1-2022",
        "end_date": "07-20-2022",
        "championshipName": "Summer 2022 League",
        "winnerTeamName": "Warriors",
        "runnerUpTeamName": "Legends",
        "winnerPlayerOne": "Zulifqar Ahmad",
        "winnerPlayerOneId": "zulifqar.ahmad.malik@gmail.com",
        "winnerPlayerTwo": "Badar Usmani",
        "winnerPlayerTwoId": "busmani84@gmail.com",
        "winnerPointsSetOne": 6,
        "winnerPointsSetTwo": 6,
        "runnerUpPlayerOne": "Adeel Taseer",
        "runnerUpPlayerOneId": "adeeltaseer@gmail.com",
        "runnerUpPlayerTwo": "Muhammad Ali Taseer",
        "runnerUpPlayerTwoId": "alitaseer@gmail.com",
        "runnerUpPointsSetOne": 4,
        "runnerUpPointsSetTwo": 3
      },
      {
        "id": 3,
        "start_date": "08-1-2020",
        "end_date": "09-20-2020",
        "championshipName": "Fall 2020 League",
        "winnerTeamName": "BA-AS",
        "runnerUpTeamName": "ZU",
        "winnerPlayerOne": "Badar Usmani",
        "winnerPlayerOneId": "busmani84@gmail.com",
        "winnerPlayerTwo": "Asif Hasan",
        "winnerPlayerTwoId": "asifh316@gmail.com",
        "runnerUpPlayerOne": "Jawad Zubairi",
        "runnerUpPlayerOneId": "jawad-zubairi",
        "runnerUpPlayerTwo": "Ali Tanveer",
        "runnerUpPlayerTwoId": "ali-tanveer"
      },
      {
        "id": 2,
        "start_date": "05-1-2020",
        "end_date": "07-20-2020",
        "championshipName": "Summer 2020 League",
        "winnerTeamName": "ZA-UT",
        "runnerUpTeamName": "Warriors",
        "winnerPlayerOne": "Zulifqar Ahmad",
        "winnerPlayerOneId": "zulifqar.ahmad.malik@gmail.com",
        "winnerPlayerTwo": "Usman Tanveer",
        "winnerPlayerTwoId": "usman.tanveer@hotmail.com",
        "runnerUpPlayerOne": "Badar Usmani",
        "runnerUpPlayerOneId": "busmani84@gmail.com",
        "runnerUpPlayerTwo": "Asif Hasan",
        "runnerUpPlayerTwoId": "asifh316@gmail.com"
      },
      {
        "id": 1,
        "start_date": "01-1-2020",
        "end_date": "04-20-2020",
        "championshipName": "Spring 2020 League",
        "winnerTeamName": "Maula Jatt",
        "runnerUpTeamName": "Warriors",
        "winnerPlayerOne": "Zulifqar Ahmad",
        "winnerPlayerOneId": "zulifqar.ahmad.malik@gmail.com",
        "winnerPlayerTwo": "Usman Qureshi",
        "winnerPlayerTwoId": "usmanqureshi1@gmail.com",
        "runnerUpPlayerOne": "Badar Usmani",
        "runnerUpPlayerOneId": "busmani84@gmail.com",
        "runnerUpPlayerTwo": "Asif Hasan",
        "runnerUpPlayerTwoId": "asifh316@gmail.com"
      }
    ];

    // Connect to PostgreSQL
    await client.connect();

    // Fetch players data from PostgreSQL
    const playersPg = (await client.query('SELECT player_id, v1_player_id FROM sports.players')).rows;
    const teamsPg = (await client.query('SELECT team_id, team_name FROM sports.teams')).rows;

    // Map and sort teams data
    const mappedTeamTableData = teamsDynamo.map(t => ({
      tournament_name: t.championshipName,
      start_date: t.start_date,
      end_date: t.end_date,
      season: t.id,
      league_id: 1,
      winner_team_id: teamsPg.find(p => p.team_name === t.winnerTeamName)?.team_id,
      runnerup_team_id: teamsPg.find(p => p.team_name === t.runnerUpTeamName)?.team_id,
      best_team_of_tournament: teamsPg.find(p => p.team_name === t.best_team_of_tournament)?.team_id,
      player_of_the_tournament: playersPg.find(p => p.v1_player_id === t.player_of_the_tournament)?.player_id,
    })).sort((a, b) => a.season - b.season);

    console.log("Mapped Team Data: ", mappedTeamTableData);

    // Insert into tournaments table
    for (const tournament of mappedTeamTableData) {
      const insertTournamentQuery = `
        INSERT INTO sports.tournaments (
          tournament_name, start_date, end_date, season, league_id, winner_team_id, 
          runnerup_team_id, player_of_the_tournament, best_team_of_tournament
        ) VALUES ($1, $2, $3, $4, $5, $6, $7, $8, $9);
      `;
      const tournamentValues = [
        tournament.tournament_name,
        tournament.start_date,
        tournament.end_date,
        tournament.season,
        tournament.league_id,
        tournament.winner_team_id,
        tournament.runnerup_team_id,
        tournament.player_of_the_tournament,
        tournament.best_team_of_tournament,
      ];
      await client.query(insertTournamentQuery, tournamentValues);
    }

    // Close the PostgreSQL connection
    await client.end();

    console.log('Data inserted successfully.');
  } catch (error) {
    console.error('Error fetching data or inserting into PostgreSQL:', error);
  }
}

// Run the function
fetchDataAndPrepareInsertQuery();
