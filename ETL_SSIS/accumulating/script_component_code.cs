// ============================================
// SSIS Script Component - Transformation
// Package: BerlinSBahn_Accumulating_Fact_Update.dtsx
// Component: Script Component
// Purpose: Update FactTrip with completion time and process hours
//
// Input Columns (ReadOnly):
//   - txn_id
//   - Copy of accm_txn_complete_time  (from Data Conversion)
// ============================================

public override void Input0_ProcessInputRow(Input0Buffer Row)
{
    string connStr = "Data Source=LAPTOP-02ACOLL8\\MSSQLSERVER2;" +
                     "Initial Catalog=BerlinSBahn_DW;" +
                     "Integrated Security=True;";

    using (System.Data.SqlClient.SqlConnection conn =
           new System.Data.SqlClient.SqlConnection(connStr))
    {
        conn.Open();

        string sql = @"UPDATE dbo.FactTrip 
                       SET accm_txn_complete_time = @ctime,
                           txn_process_time_hours = DATEDIFF(HOUR, accm_txn_create_time, @ctime)
                       WHERE trip_id = @tid";

        System.Data.SqlClient.SqlCommand cmd =
            new System.Data.SqlClient.SqlCommand(sql, conn);

        cmd.Parameters.AddWithValue("@ctime", Row.Copyofaccmtxncompletetime);
        cmd.Parameters.AddWithValue("@tid",   Row.txnid);

        cmd.ExecuteNonQuery();
    }
}
