const axios = require('axios');
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
    const teamsFromDynamo = await axios.get('https://prod.roundrocktennis.com/v1/teams');
    const teamsDynamo = teamsFromDynamo.data;

    // Connect to PostgreSQL
    await client.connect();

    // Fetch players data from PostgreSQL
    const playersPg = (await client.query('SELECT player_id, v1_player_id FROM sports.players')).rows;

    // Map and sort teams data
    const mappedTeamTableData = teamsDynamo.map(t => ({
      team_name: t.teamName,
      captain_id: playersPg.find(p => p.v1_player_id === t.teamCaptainId)?.player_id ,
      vice_captain_id: null, // Assuming no vice-captain data in DynamoDB
      status: t.isActiveTeam ? "ACTIVE" : "INACTIVE",
      created_at: t.createdAt,
      created_by: t.createdBy,
      v1_team_id: t.teamId,
      teamPlayersIds:t.teamPlayersIds
    })).sort((a, b) => a.v1_team_id - b.v1_team_id);


    console.log("Mpped Pteam ", mappedTeamTableData[10])

    // Prepare and execute insert queries for teams
    for (const mappedTeam of mappedTeamTableData) {
      const insertTeamQuery = `
        INSERT INTO sports.teams (v1_team_id, team_name, game_id, captain, vice_captain, status, created_at, created_by)
        VALUES ($1, $2, $3, $4, $5, $6, $7, $8)
        RETURNING team_id;
      `;
      const teamValues = [mappedTeam.v1_team_id, mappedTeam.team_name,1, mappedTeam.captain_id, mappedTeam.vice_captain_id, mappedTeam.status, mappedTeam.created_at, mappedTeam.created_by];
      const res = await client.query(insertTeamQuery, teamValues);

      const teamId = res.rows[0].team_id;

      // Insert player-team relationships
      for (const playerId of mappedTeam.teamPlayersIds) {
        const playerIdPg = playersPg.find(p => p.v1_player_id === playerId)?.player_id;
        if (playerIdPg) {
          const insertPlayerTeamQuery = `
            INSERT INTO sports.player_teams (player_id, team_id)
            VALUES ($1, $2);
          `;
          await client.query(insertPlayerTeamQuery, [playerIdPg, teamId]);
        }
      }
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
