using System;
using System.Collections.Generic;
using System.Linq;
using System.Text;
using System.Threading.Tasks;
using System.Data.SqlClient;
using System.IO;

namespace SettlementLoader
{
    class FileLoader
    {
        public static void ProcessFiles()
        {
            string sSQL = "";

            // Open database connection
            try
            {
                using (SqlConnection connection = new SqlConnection(Properties.Settings.Default.DatabaseConnectionString))
                {
                    connection.Open();
                    
                    // Process downloaded files ready for load
                    sSQL = "SELECT file_transfer_task_id, file_transfer_task.destination_address, destination_filename, source_name, transfer_method_cd" + Environment.NewLine;
                    sSQL += "FROM etl.file_transfer_task, etl.file_transfer_source" + Environment.NewLine;
                    sSQL += "WHERE download_status_cd IN ('FTTD_DOWNLOADED')" + Environment.NewLine;
                    sSQL += "    AND transfer_method_cd IN ('TM_MSRS_HTTP', 'TM_MSRS_BILL_HTTP', 'TM_INSCHEDULES')" + Environment.NewLine;
                    sSQL += "    AND file_transfer_source.file_transfer_source_id = file_transfer_task.file_transfer_source_id" + Environment.NewLine;
                    sSQL += "    AND (load_status_cd IS NULL" + Environment.NewLine;
                    sSQL += "        OR load_status_cd IN ('FTTL_READY', 'FTTL_RETRY', 'FTTL_STATUS_NEW'))" + Environment.NewLine;
                    using (SqlCommand cmd = new SqlCommand(sSQL,connection))
                    {
                        using (SqlDataReader dr = cmd.ExecuteReader())
                        {
                            while (dr.Read())
                            {
                                Program.UpdateTaskStatus(Convert.ToInt64(dr["file_transfer_task_id"]), loadStatusCode: "FTTD_LOADING");

                                bool result = false;

                                if (dr["transfer_method_cd"].ToString() == "TM_MSRS_HTTP")
                                {
                                    FileLoader fileloader = new FileLoader();
                                    result = fileloader.LoadFileReport(Convert.ToInt64(dr["file_transfer_task_id"]), dr["destination_address"].ToString() + dr["destination_filename"].ToString(), dr["source_name"].ToString());
                                }
                                else if (dr["transfer_method_cd"].ToString() == "TM_MSRS_BILL_HTTP")
                                {
                                    FileLoader fileloader = new FileLoader();
                                    result = fileloader.LoadFileBill(Convert.ToInt64(dr["file_transfer_task_id"]), dr["destination_address"].ToString() + dr["destination_filename"].ToString());
                                }
                                else if (dr["transfer_method_cd"].ToString() == "TM_INSCHEDULES")
                                {
                                    FileLoader fileloader = new FileLoader();
                                    result = fileloader.LoadFileInSchedulesLosses(Convert.ToInt64(dr["file_transfer_task_id"]), dr["destination_address"].ToString() + dr["destination_filename"].ToString());
                                }

                                if (result)
                                {
                                    Program.UpdateTaskStatus(Convert.ToInt64(dr["file_transfer_task_id"]), loadStatusCode: "FTTL_LOADED");
                                }
                                else
                                {
                                    // TODO:  implement retry logic
                                    Program.UpdateTaskStatus(Convert.ToInt64(dr["file_transfer_task_id"]), loadStatusCode: "FTTL_ERROR");
                                }
                            }
                        }
                    }
                }
            }
            catch (Exception ex)
            {
                Console.WriteLine("Error:" + ex.Message);
                Program.LogError(Properties.Settings.Default.TaskName + ":ProcessFiles", sSQL, ex);
            }
        }
        private bool LoadFileReport(long fileTransferTaskID, string pathName, string sourceName)
        {
            string sSQL = "";

            try
            {
                using (SqlConnection connection = new SqlConnection(Properties.Settings.Default.DatabaseConnectionString))
                {
                    connection.Open();

                    // open downloaded file
                    using (StreamReader sr = new StreamReader(pathName))
                    {
                        string currentLine;
                        string[] columnHeaders;
                        string[] columnData;
                        string sqlInsert;

                        // rely on null exception if the file is mal-formatted
                        currentLine = sr.ReadLine(); // skip report title
                        currentLine = sr.ReadLine(); // skip customer account and report creation timestamp
                        currentLine = sr.ReadLine(); // skip start and end date of report
                        currentLine = sr.ReadLine(); // skip column number tags

                        currentLine = sr.ReadLine(); // get column headers from file, will be used to generate INSERT statement
                        columnHeaders = Program.SplitRow(currentLine).ToArray();
                        RenameColumnHeaders(ref columnHeaders); // replace invalid column headers with actual database column headers
                        sqlInsert = GenerateInsertSQL(columnHeaders, sourceName);

                        // currentLine will be null when the StreamReader reaches the end of file
                        while ((currentLine = sr.ReadLine()) != null)
                        {
                            //Console.WriteLine(currentLine);
                            if (currentLine.Contains(",")) // skip final line in file "End of Report"
                            {
                                columnData = Program.SplitRow(currentLine).ToArray();
                                sSQL = sqlInsert + " VALUES (" + fileTransferTaskID + ", " + GenerateDataSQL(columnData);
                                using (SqlCommand cmd = new SqlCommand(sSQL,connection))
                                {
                                    int result = cmd.ExecuteNonQuery();
                                }
                            }
                        }
                        return true;
                    }
                }
            }
            catch (Exception ex)
            {
                Console.WriteLine("Error:" + ex.Message);
                Program.LogError(Properties.Settings.Default.TaskName + ":LoadFile", sSQL, ex);
                return false;
            }
        }
        private static void RenameColumnHeaders(ref string[] columnHeaders)
        {
            for (int i = 0; i < columnHeaders.Length; i++)
            {
                columnHeaders[i] = columnHeaders[i].Replace("EPT HE 02*", "EPT HE 02DST");
                columnHeaders[i] = columnHeaders[i].Replace("Total Zone Synch Reserve Lost Opportunity Cost Credit Cleared ($)", "Total Zone Synch Reserve Lost Opportunity Cost Credit Cleared ($");
            }
        }
        private static string GenerateInsertSQL(string[] columnHeaders, string sourceName)
        {
            string insertSQL = "INSERT INTO [etl].[" + sourceName + "] (" + Environment.NewLine + "file_transfer_task_id, ";

            for (int i = 0; i < columnHeaders.Length; i++)
            {
                insertSQL += "[" + columnHeaders[i] + "]";
                if (i != columnHeaders.Length - 1) insertSQL += ", ";
            }

            insertSQL += Environment.NewLine + ")" + Environment.NewLine;
            return insertSQL;
        }
        private static string GenerateDataSQL(string[] columnData)
        {
            string valuesSQL =""; //= "VALUES (" + fileTransferTaskID + ", ";
            for (int i = 0; i < columnData.Length; i++)
            {
                if (Program.IsNumeric(columnData[i]))
                {
                    valuesSQL += columnData[i];
                }
                else
                {
                    if (columnData[i] == "")
                    {
                        valuesSQL += "NULL";
                    }
                    else
                    {
                        valuesSQL += "'" + columnData[i] + "'";
                    }
                }
                if (i != columnData.Length - 1) valuesSQL += ", ";
            }
            valuesSQL += ")" + Environment.NewLine;
            return valuesSQL;
        }
        private bool LoadFileBill(long fileTransferTaskID, string pathName)
        {
            string sSQL = "";

            try
            {
                using (SqlConnection connection = new SqlConnection(Properties.Settings.Default.DatabaseConnectionString))
                {
                    connection.Open();
                    SqlTransaction tran = connection.BeginTransaction();

                    // open downloaded file
                    using (StreamReader sr = new StreamReader(pathName))
                    {
                        string currentLine;
                        string[] columns;
                        long billID;
                        bool hadDetail = false;

                        sSQL = "SET NOCOUNT ON; INSERT INTO etl.msrs_BILLCSVXML ([REPORT NAME], [CUSTOMER ACCOUNT], " + Environment.NewLine;
                        sSQL += "[CUSTOMER CODE], [CUSTOMER ID], [FINAL BILLING STATEMENT ISSUED], [BILLING PERIOD START DATE], " + Environment.NewLine;
                        sSQL += "[BILLING PERIOD END DATE], [INVOICE NUMBER], [INVOICE DUE DATE], file_transfer_task_id) VALUES (" + Environment.NewLine;

                        // rely on null exception if the file is mal-formatted
                        // get the invoice header data elements, there are 9 rows of header fields in the file
                        for (int i = 1; i < 10; i++)
                        {
                            currentLine = sr.ReadLine();
                            sSQL += Program.IsEmptyWithQuotes(Program.SplitRow(currentLine).ToArray()[1].ToString()) + "," + Environment.NewLine;
                        }

                        // create bill/invoice header record and get ID to use on the detail inserts and total lines that follow
                        sSQL += fileTransferTaskID + "); SELECT SCOPE_IDENTITY();";
                        using (SqlCommand cmd = new SqlCommand(sSQL, connection, tran))
                        {
                            billID = Convert.ToInt64(cmd.ExecuteScalar());
                        }

                        currentLine = sr.ReadLine();    // skip blank row
                        currentLine = sr.ReadLine();    // skip column headers for CHARGE detail

                        double totalCharges = 0;
                        double totalCredits = 0;
                        double monthlyBillingNetTotal = 0;
                        double previousWeeklyBillingNetTotal = 0;
                        double totalDue = 0;

                        while ((currentLine = sr.ReadLine()) != null)
                        {
                            if (currentLine.Contains(",")) // skip final line in file "End of Report" and skip blank rows.  if it has a comma, it has at least 5 columns
                            {
                                columns = Program.SplitRow(currentLine).ToArray();
                                if (Program.IsNumeric(columns[0]))  // check for presence of a line item, otherwise a total line or column header row
                                {
                                    sSQL = "INSERT INTO etl.msrs_BILLCSVXML_detail (charge_code, adjustment_code, billing_line_item_name, " + Environment.NewLine;
                                    sSQL += "  source_billing_period_start, amount, msrs_BILLCSVXML_id) VALUES (" + Environment.NewLine;
                                    sSQL += " '" + columns[0].ToString().Replace("'", "''") + "'," + Environment.NewLine;  // charge code
                                    sSQL += " " + Program.IsEmptyWithQuotes(columns[1].ToString().Replace("'", "''")) + "," + Environment.NewLine;  // adjustment code
                                    sSQL += " '" + columns[2].ToString().Replace("'", "''") + "'," + Environment.NewLine;  // billing line item name
                                    sSQL += " " + Program.IsEmptyWithQuotes(columns[3].ToString()) + "," + Environment.NewLine;  // source billing period start
                                    sSQL += " " + columns[4] + "," + Environment.NewLine;  // amount
                                    sSQL += " " + billID + ")" + Environment.NewLine;

                                    using (SqlCommand cmd = new SqlCommand(sSQL,connection,tran))
                                    {
                                        int result = cmd.ExecuteNonQuery();
                                        hadDetail = true;
                                    }
                                }
                                else if (columns[2].ToString().Contains("Total Charges")) totalCharges = Convert.ToDouble(columns[4]);
                                else if (columns[2].ToString().Contains("Total Credits")) totalCredits = Convert.ToDouble(columns[4]);
                                else if (columns[2].ToString().Contains("Monthly Billing Net Total")) monthlyBillingNetTotal = Convert.ToDouble(columns[4]);
                                else if (columns[2].ToString().Contains("Previous Weekly Billing Net Total")) previousWeeklyBillingNetTotal = Convert.ToDouble(columns[4]);
                                else if (columns[2].ToString().Contains("Total Due")) totalDue = Convert.ToDouble(columns[4]);
                            }
                        }

                        if (hadDetail)
                        {
                            // update the settlement statement with the total lines found while processing the detail lines
                            sSQL = "UPDATE etl.msrs_BILLCSVXML SET [Total Charges] = " + totalCharges + "," + Environment.NewLine;
                            sSQL += "  [Total Credits] = " + totalCredits + "," + Environment.NewLine;
                            sSQL += "  [Monthly Billing Net Total] = " + monthlyBillingNetTotal + "," + Environment.NewLine;
                            sSQL += "  [Previous Weekly Billing Net Total] = " + previousWeeklyBillingNetTotal + "," + Environment.NewLine;
                            sSQL += "  [Total Due] = " + totalDue + Environment.NewLine;
                            sSQL += "WHERE msrs_BILLCSVXML_id = " + billID + Environment.NewLine;
                            using (SqlCommand cmd = new SqlCommand(sSQL, connection,tran))
                            {
                                int result = cmd.ExecuteNonQuery();
                                tran.Commit();
                            }
                        }
                        else
                        {
                            // empty file probably, still pass back "true" because it was successfully processed although empty, but rollback the partial bill creation
                            tran.Rollback();
                        }
                    }
                }
                return true;
            }
            catch (Exception ex)
            {
                // todo:  handle the rollback, put the transaction in another try inside the connection using statement
                Console.WriteLine("Error:" + ex.Message);
                Program.LogError(Properties.Settings.Default.TaskName + ":LoadFile", sSQL, ex);
                return false;
            }
        }
        private bool LoadFileInSchedulesLosses(long fileTransferTaskID, string pathName)
        {
            string sSQL = "";

            try
            {
                using (SqlConnection connection = new SqlConnection(Properties.Settings.Default.DatabaseConnectionString))
                {
                    connection.Open();

                    // open downloaded file
                    using (StreamReader sr = new StreamReader(pathName))
                    {
                        string currentLine;
                        string[] columnData;

                        // rely on null exception if the file is mal-formatted
                        currentLine = sr.ReadLine(); // skip column headers

                        // currentLine will be null when the StreamReader reaches the end of file
                        while ((currentLine = sr.ReadLine()) != null)
                        {
                            //Console.WriteLine(currentLine);
                            if (currentLine.Contains(",")) // skip final line in file "End of Report" or any other blank line
                            {
                                columnData = Program.SplitRow(currentLine).ToArray();
                                sSQL = "INSERT INTO etl.inSch_edclossfactor (file_transfer_task_id, date_time_stamp, [day], [hour_ending]," + Environment.NewLine;
                                sSQL += "[dst], [EDC], [LOSS_DERATION_FACTOR]) VALUES (" + fileTransferTaskID + ", '" + Convert.ToDateTime(columnData[0]).AddHours(Convert.ToInt16(columnData[1])) + "'," + Environment.NewLine;
                                sSQL = sSQL + GenerateDataSQL(columnData);
                                try
                                {
                                    using (SqlCommand cmd = new SqlCommand(sSQL, connection))
                                    {
                                        int result = cmd.ExecuteNonQuery();
                                    }
                                }
                                catch (Exception ex)
                                { //TODO: update the record in the future.}
                                    //TODO:  allow sqlclient.sqlexception to continue, or replace with MERGE statement
                                    Console.WriteLine("ERROR:" + ex.Message);
                                    //return false;
                                }
                            }
                        }
                        return true;
                    }
                }
            }
            catch (Exception ex)
            {
                Console.WriteLine("Error:" + ex.Message);
                Program.LogError(Properties.Settings.Default.TaskName + ":LoadFile", sSQL, ex);
                return false;
            }
        }
    }

}
