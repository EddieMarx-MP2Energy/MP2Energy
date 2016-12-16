using System;
using System.Collections.Generic;
using System.Linq;
using System.Text;
using System.Threading.Tasks;
using System.Data.SqlClient;

namespace SettlementLoader
{
    class Program
    {
        static void Main(string[] args)
        {
            DownloadManager.ProcessDownloads();
            FileLoader.ProcessFiles();
        }

        public static void UpdateTaskStatus(long fileTransferTaskID, string downloadStatusCode = "", string loadStatusCode = "", string sourceFileName = "", string destinationFileName = "", long fileSize = 0)
        {
            string sSQL;

            // open the database connection
            using (SqlConnection connection = new SqlConnection(Properties.Settings.Default.DatabaseConnectionString))
            {
                connection.Open();

                // update the etl.file_transfer_task record, only updating the parameters present in the call
                sSQL = "UPDATE etl.file_transfer_task SET " + Environment.NewLine;

                if (downloadStatusCode != "") sSQL += "download_status_cd = '" + downloadStatusCode + "'," + Environment.NewLine;
                if (loadStatusCode != "") sSQL += "load_status_cd = '" + loadStatusCode + "'," + Environment.NewLine;
                if (sourceFileName != "") sSQL += "source_filename = '" + sourceFileName + "'," + Environment.NewLine;
                if (destinationFileName != "") sSQL += "destination_filename = '" + destinationFileName + "'," + Environment.NewLine;
                if (fileSize != 0) sSQL += "file_size_bytes = " + fileSize.ToString() + "," + Environment.NewLine;

                sSQL += "machine_name = '" + Environment.MachineName + "'," + Environment.NewLine;
                sSQL += "update_date = getdate(), update_user = user_name()" + Environment.NewLine;
                sSQL += "WHERE file_transfer_task_id = " + fileTransferTaskID.ToString();

                using (SqlCommand cmd = new SqlCommand(sSQL, connection))
                {
                    cmd.CommandType = System.Data.CommandType.Text;
                    int result = cmd.ExecuteNonQuery();
                    Console.WriteLine("updated " + result + " file_transfer_task records:" + downloadStatusCode + " " + loadStatusCode);
                }
            }
        }
        public static bool IsNumeric(object Expression)
        {
            // http://stackoverflow.com/questions/894263/how-do-i-identify-if-a-string-is-a-number
            double retNum;

            bool isNum = Double.TryParse(Convert.ToString(Expression), System.Globalization.NumberStyles.Any, System.Globalization.NumberFormatInfo.InvariantInfo, out retNum);
            return isNum;
        }
        public static IEnumerable<string> SplitRow(string row, char delimiter = ',')
        {
            // http://stackoverflow.com/questions/3776458/split-a-comma-separated-string-with-both-quoted-and-unquoted-strings
            var currentString = new StringBuilder();
            var inQuotes = false;
            var quoteIsEscaped = false; //Store when a quote has been escaped.
            row = string.Format("{0}{1}", row, delimiter); //We add new cells at the delimiter, so append one for the parser.
            foreach (var character in row.Select((val, index) => new { val, index }))
            {
                if (character.val == delimiter) //We hit a delimiter character...
                {
                    if (!inQuotes) //Are we inside quotes? If not, we've hit the end of a cell value.
                    {
                        //Console.WriteLine(currentString);
                        yield return currentString.ToString();
                        currentString.Clear();
                    }
                    else
                    {
                        currentString.Append(character.val);
                    }
                }
                else
                {
                    if (character.val != ' ')
                    {
                        if (character.val == '"') //If we've hit a quote character...
                        {
                            if (character.val == '\"' && inQuotes) //Does it appear to be a closing quote?
                            {
                                if (row[character.index + 1] == character.val) //If the character afterwards is also a quote, this is to escape that (not a closing quote).
                                {
                                    quoteIsEscaped = true; //Flag that we are escaped for the next character. Don't add the escaping quote.
                                }
                                else if (quoteIsEscaped)
                                {
                                    quoteIsEscaped = false; //This is an escaped quote. Add it and revert quoteIsEscaped to false.
                                    currentString.Append(character.val);
                                }
                                else
                                {
                                    inQuotes = false;
                                }
                            }
                            else
                            {
                                if (!inQuotes)
                                {
                                    inQuotes = true;
                                }
                                else
                                {
                                    currentString.Append(character.val); //...It's a quote inside a quote.
                                }
                            }
                        }
                        else
                        {
                            currentString.Append(character.val);
                        }
                    }
                    else
                    {
                        if (!string.IsNullOrWhiteSpace(currentString.ToString())) //Append only if not new cell
                        {
                            currentString.Append(character.val);
                        }
                    }
                }
            }
        }
        public static void LogSession(string taskName, string taskDetail, DateTime startTime)
        {
            try
            {
                using (SqlConnection connection = new SqlConnection(Properties.Settings.Default.DatabaseConnectionString))
                {
                    connection.Open();

                    using (SqlCommand cmd = new SqlCommand("[ESG].[Trace].[sp_Session]", connection))
                    {
                        cmd.CommandType = System.Data.CommandType.StoredProcedure;
                        cmd.Parameters.Add(new SqlParameter("pApplicationID", Properties.Settings.Default.ApplicationID));
                        cmd.Parameters.Add(new SqlParameter("pTaskName", taskName.Replace("'", "''")));
                        cmd.Parameters.Add(new SqlParameter("pTaskDetail", taskDetail.Replace("'", "''")));
                        cmd.Parameters.Add(new SqlParameter("pStart",startTime));
                        cmd.Parameters.Add(new SqlParameter("pUpdater", Environment.UserName.Replace("'", "''")));
                        cmd.ExecuteNonQuery();
                    }
                }
            }
            catch (Exception ex)
            {
                Console.WriteLine("Error attempting to call sp_Session:" + ex.Message);
            }
        }
        public static void LogError(string taskName, string taskDetail, System.Exception exception)
        {
            try
            {
                using (SqlConnection connection = new SqlConnection(Properties.Settings.Default.DatabaseConnectionString))
                {
                    connection.Open();

                    using (SqlCommand cmd = new SqlCommand("[ESG].[Trace].[sp_Error_PUT]", connection))
                    {
                        cmd.CommandType = System.Data.CommandType.StoredProcedure;
                        cmd.Parameters.Add(new SqlParameter("pApplicationID", Properties.Settings.Default.ApplicationID));
                        cmd.Parameters.Add(new SqlParameter("pTaskName", taskName.Replace("'", "''")));
                        cmd.Parameters.Add(new SqlParameter("pMessage", exception.Message.Replace("'", "''")));
                        cmd.Parameters.Add(new SqlParameter("pStackTrace", exception.StackTrace.ToString().Replace("'", "''")));
                        cmd.Parameters.Add(new SqlParameter("pNumber", 1));
                        cmd.Parameters.Add(new SqlParameter("pTaskDetail", taskDetail.Replace("'", "''")));
                        cmd.Parameters.Add(new SqlParameter("pUpdater", Environment.UserName.Replace("'", "''")));
                        cmd.Parameters.Add(new SqlParameter("pShownToUser", 0));
                        cmd.Parameters.Add(new SqlParameter("pUpdateTS", null));
                        cmd.ExecuteNonQuery();
                    }
                }
            }
            catch (Exception ex)
            {
                Console.WriteLine("Error attempting to call sp_Error_PUT:" + ex.Message + ", logging exception:" + exception.Message);
            }
        }
        public static string IsEmptyWithQuotes(string inString)
        {
            if (inString == "")
            {
                return "NULL";
            }
            {
                return "'" + inString + "'";
            }
        }
    }
}
