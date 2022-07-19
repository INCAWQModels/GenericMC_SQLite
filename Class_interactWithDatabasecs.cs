using System;
using System.Collections.Generic;
using System.Linq;
using System.Text;
using System.Data;
using System.Data.SQLite;

namespace MC
{
    class resultsDatabase
    {
        protected SQLiteConnection connection;

        protected resultsDatabase()
        {
            connection = new SQLiteConnection();
        }

        protected void connect()
        {
            string localConnectionString = "Driver={Microsoft Access Driver (*.mdb, *.accdb)}; Dbq=" + MCParameters.databaseFileName + ";Uid=Admin;Pwd=;";
            connection.ConnectionString = localConnectionString;
            connection.Open();
        }
    }
}
