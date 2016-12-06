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
            string sSQL="";

            // Open database connection
            try
            {
                using (var connection = new SqlConnection(Properties.Settings.Default.DatabaseConnectionString))
                {
                    connection.Open();
                    // Process downloaded files ready for load
                    using (SqlCommand cmd = new SqlCommand())
                    {
                        sSQL = "SELECT file_transfer_task_id, file_transfer_task.destination_address, destination_filename, source_name" + Environment.NewLine;
                        sSQL += "FROM etl.file_transfer_task, etl.file_transfer_source" + Environment.NewLine;
                        sSQL += "WHERE download_status_cd IN ('FTTD_DOWNLOADED')" + Environment.NewLine;
                        sSQL += "    AND transfer_method_cd = 'TM_MSRS_HTTP'" + Environment.NewLine;
                        sSQL += "    AND file_transfer_source.file_transfer_source_id = file_transfer_task.file_transfer_source_id" + Environment.NewLine;
                        sSQL += "    AND (load_status_cd IS NULL" + Environment.NewLine;
                        sSQL += "        OR load_status_cd IN ('FTTL_READY', 'FTTL_RETRY', 'FTTL_STATUS_NEW'))" + Environment.NewLine;

                        cmd.Connection = connection;
                        cmd.CommandText = sSQL;

                        using (SqlDataReader dr = cmd.ExecuteReader())
                        {
                            while (dr.Read())
                            {
                                Program.UpdateTaskStatus(Convert.ToInt64(dr["file_transfer_task_id"]), loadStatusCode: "FTTD_LOADING");

                                FileLoader fileloader = new FileLoader();
                                bool result = fileloader.LoadFile(Convert.ToInt64(dr["file_transfer_task_id"]), dr["destination_address"].ToString() + dr["destination_filename"].ToString(), dr["source_name"].ToString());

                                if (result)
                                {
                                    Program.UpdateTaskStatus(Convert.ToInt64(dr["file_transfer_task_id"]), loadStatusCode: "FTTD_LOADED");
                                }
                                else
                                {
                                    // TODO:  implement retry logic
                                    Program.UpdateTaskStatus(Convert.ToInt64(dr["file_transfer_task_id"]), loadStatusCode: "FTTD_ERROR");
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
        private bool LoadFile(long fileTransferTaskID, string pathName, string sourceName)
        {
            string sSQL="";

            try
            {
                using (var connection = new SqlConnection(Properties.Settings.Default.DatabaseConnectionString))
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
                                using (SqlCommand cmd = new SqlCommand())
                                {
                                    sSQL = sqlInsert + GenerateDataSQL(columnData, fileTransferTaskID);
                                    cmd.Connection = connection;
                                    cmd.CommandText = sSQL;
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
                if (i != columnHeaders.Length-1) insertSQL += ", ";
            }

            insertSQL += Environment.NewLine + ")" + Environment.NewLine;
            return insertSQL;
        }
        private static string GenerateDataSQL(string[] columnData, long fileTransferTaskID)
        {
            string valuesSQL = "VALUES (" + fileTransferTaskID + ", ";
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
                if (i != columnData.Length-1) valuesSQL += ", ";
            }
            valuesSQL += ")" + Environment.NewLine;
            return valuesSQL;
        }
    }
}
