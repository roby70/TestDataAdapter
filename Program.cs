// See https://aka.ms/new-console-template for more information
using System.Data;
using System.Data.SqlClient;


// ripulisco MyTable con un "DELETE MyTable"
using var conn = new SqlConnection("Server=(localdb)\\MSSQLLocalDB;Database=Test;Trusted_Connection=True;");
conn.Open();
using (SqlCommand cmd = new SqlCommand("DELETE MyTable", conn)) {
    cmd.ExecuteNonQuery();
}

// create a DataTable object with 3 columns MainId (as integer), DetailId (as string), Description (as string)
DataTable dt = new DataTable();
dt.Columns.Add("MainId", typeof(int));
dt.Columns.Add("DetailId", typeof(string));
dt.Columns.Add("Description", typeof(string));

// set MainId and DetailId as primary key
dt.PrimaryKey = new DataColumn[] { dt.Columns["MainId"], dt.Columns["DetailId"] };

// add 3 rows
dt.Rows.Add(1, "A", "Description A");
dt.Rows.Add(1, "B", "Description B");
dt.Rows.Add(2, "A", "Description A");

// create a SqlAdapter object to update the table MyTable in the database with new data
// using the keys defined in the DataTable
using (SqlDataAdapter adapter = new SqlDataAdapter("SELECT * FROM MyTable", conn)) {
    // create a SqlCommandBuilder object to generate the SQL commands for updating the database
    using (SqlCommandBuilder builder = new SqlCommandBuilder(adapter)) {
        // update the database
        adapter.Update(dt);
    }
}

// carico i dati da MyTable tramite un data reader in una nuova tabella dt2
DataTable dt2 = new DataTable();

using (SqlCommand cmd = new SqlCommand("SELECT MainId, DetailId, Description FROM MyTable", conn))
using (SqlDataReader reader = cmd.ExecuteReader()) {
            dt2.Load(reader);
}

// aggiorno la riga con MainId = 1 e DetailId = B con una nuova descrizione
dt2.Select("MainId = 1 AND DetailId = 'B'")[0]["Description"] = "New description";
// aggiungo una riga
dt2.Rows.Add(3, "A", "Description A");
// rimuovo la riga MainId 2 e DetailId A
dt2.Select("MainId = 2 AND DetailId = 'A'")[0].Delete();

// creo un dizionario con chiave columnName e come valore SqlDbType e Size
var columnTypes = new Dictionary<string, Tuple<SqlDbType, int>>();

// get SqlDbType and size from INFORMATION_SCHEMA.COLUMNS for columns Description, MainId and DetailId from table MyTable
using (SqlCommand cmd = new SqlCommand("SELECT COLUMN_NAME, DATA_TYPE, CHARACTER_MAXIMUM_LENGTH FROM INFORMATION_SCHEMA.COLUMNS WHERE TABLE_NAME = 'MyTable' AND COLUMN_NAME IN ('Description', 'MainId', 'DetailId')", conn))
using (SqlDataReader reader = cmd.ExecuteReader()) {
    while (reader.Read()) {
        var columnName = reader.GetString(0);
        var dataType = reader.GetString(1);
        // trasformo in sqldatatype
        SqlDbType sqlDataType = (SqlDbType)Enum.Parse(typeof(SqlDbType), dataType, true);
        var size = reader.IsDBNull(2) ? 0 : reader.GetInt32(2);
        // add to dictionary
        columnTypes.Add(columnName, new Tuple<SqlDbType, int>(sqlDataType, size));
    }
}




// aggiorno i dati tramite data adapter
using (SqlDataAdapter adapter = new SqlDataAdapter("SELECT MainId, DetailId, Description FROM MyTable", conn)) {
    
    // manually set an update command for MyTable using MainId and DetailId as primary key
    adapter.UpdateCommand = new SqlCommand("UPDATE MyTable SET Description = @Description WHERE MainId = @MainId AND DetailId = @DetailId", conn);
    adapter.UpdateCommand.Parameters.Add("@Description", columnTypes["Description"].Item1, columnTypes["Description"].Item2, "Description");
    adapter.UpdateCommand.Parameters.Add("@MainId", columnTypes["MainId"].Item1, columnTypes["MainId"].Item2, "MainId");
    adapter.UpdateCommand.Parameters.Add("@DetailId", columnTypes["DetailId"].Item1, columnTypes["DetailId"].Item2, "DetailId");

    adapter.InsertCommand = new SqlCommand("INSERT INTO MyTable (MainId, DetailId, Description) VALUES (@MainId, @DetailId, @Description)", conn);
    adapter.InsertCommand.Parameters.Add("@Description", columnTypes["Description"].Item1, columnTypes["Description"].Item2, "Description");
    adapter.InsertCommand.Parameters.Add("@MainId", columnTypes["MainId"].Item1, columnTypes["MainId"].Item2, "MainId");
    adapter.InsertCommand.Parameters.Add("@DetailId", columnTypes["DetailId"].Item1, columnTypes["DetailId"].Item2, "DetailId");

    adapter.DeleteCommand = new SqlCommand("DELETE MyTable WHERE MainId = @MainId AND DetailId = @DetailId", conn);
    adapter.DeleteCommand.Parameters.Add("@MainId", columnTypes["MainId"].Item1, columnTypes["MainId"].Item2, "MainId");
    adapter.DeleteCommand.Parameters.Add("@DetailId", columnTypes["DetailId"].Item1, columnTypes["DetailId"].Item2, "DetailId");

    adapter.Update(dt2);
}