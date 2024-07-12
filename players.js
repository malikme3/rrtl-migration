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
    // Fetch players data
    const response = await axios.get('https://prod.roundrocktennis.com/v1/players');
    const players = response.data;

    // Map and sort players
    const mappedPlayers = players.filter(p => !p.playerId.includes('tbd')).map(p => ({
        v1_id: p.id,
        allow_sms: false,
        created_by: p.createdBy || 'system', // Default to 'system' if undefined
        created_at: p.createdAt || new Date().toISOString(), // Use current date if undefined
        personal_notes: p.description || '', // Default to empty string if undefined
        first_name: p.fullName || '', // Default to empty string if undefined
        last_name: p.lastName || '', // Default to empty string if undefined
        v1_player_id: p.playerId || null, // Default to null if undefined
        avatar_url: p.avatarImgUrl || '', // Default to empty string if undefined
        contact_email: p.email || '', // Default to empty string if undefined
        contact_phone: p.phone || '' // Default to empty string if undefined
      })).sort((a, b) => a.v1_id - b.v1_id);

    // Prepare insert query
    const insertQuery = 'INSERT INTO sports.players (v1_id, allow_sms, created_by, created_at, personal_notes, first_name, last_name, v1_player_id, avatar_url, contact_email, contact_phone) VALUES ';
    const values = mappedPlayers.map(player => `(${player.v1_id}, ${player.allow_sms}, '${player.created_by}', '${player.created_at}', '${player.personal_notes}', '${player.first_name}', '${player.last_name}', '${player.v1_player_id}', '${player.avatar_url}', '${player.contact_email}', '${player.contact_phone}')`).join(',');

    const finalQuery = insertQuery + values + ';';

    // console.log(finalQuery);

    // Connect to PostgreSQL and execute the query
    await client.connect();
   const d = await client.query( finalQuery || 'select  * from players p2 where p2.v1_id = 1');
    await client.end();
    console.log("query response: ", d);
    console.log('Data inserted successfully.');
  } catch (error) {
    console.error('Error fetching data or inserting into PostgreSQL:', error);
  }
}

// Run the function
fetchDataAndPrepareInsertQuery();
