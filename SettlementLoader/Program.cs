using System;
using System.Collections.Generic;
using System.Linq;
using System.Text;
using System.Data.SqlClient;
using System.IO;
using System.IO.Compression;
using System.Net;
using System.Net.Mail;

namespace SettlementLoader
{
    class Program
    {
        static void Main(string[] args)
        {
            DownloadManager.CreateTransferTasksMSRS();
            DownloadManager.CreateTransferTasksERCOT();
            DownloadManager.ProcessDownloads();
            Program.ProcessZipFiles();
            FileLoader.ProcessFiles();

            // pause for ENTER key to prevent error messages from clearing after program ends
            Console.Beep(1000,5000);
            System.Media.SystemSounds.Beep.Play();
            //SendAttachmentViaEmail("eddie.marx@mp2energy.com", "Test Subject", "Test Body", "C:\\Users\\eddie.marx\\Documents\\apx\\msrs\\browserless-do-http-get-call-instructions.pdf"); // sample call, random document
            Console.WriteLine("PRESS ENTER to close window");
            Console.ReadLine();
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
                    cmd.CommandTimeout = 30; // 5 minutes, due to problems with timeout
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
                        cmd.Parameters.Add(new SqlParameter("pStart", startTime));
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
                return "'" + inString.Replace("'", "''") + "'";
            }
        }
        public static List<FileList> UnzipFile(string zipPath, string extractPath, string sourceName, long fileTransferTaskID)
        {
            List<FileList> fileList = new List<FileList>();

            using (ZipArchive archive = ZipFile.OpenRead(zipPath))
            {
                foreach (ZipArchiveEntry entry in archive.Entries)
                {
                    File.Delete((Path.Combine(extractPath, entry.FullName)));
                    File.Delete(Path.Combine(extractPath, sourceName + "_" + fileTransferTaskID + "_" + entry.FullName));
                    entry.ExtractToFile(Path.Combine(extractPath, entry.FullName));
                    File.Move(Path.Combine(extractPath, entry.FullName), Path.Combine(extractPath, sourceName + "_" + fileTransferTaskID + "_" + entry.FullName));

                    FileList i = new FileList();
                    i.origFileName = entry.FullName;
                    i.filePath = extractPath;
                    i.fileName = sourceName + "_" + fileTransferTaskID + "_" + entry.FullName;
                    i.fileSize = new System.IO.FileInfo(Path.Combine(extractPath, sourceName + "_" + fileTransferTaskID + "_" + entry.FullName)).Length;
                    fileList.Add(i);
                }
            }
            return fileList;
        }
        public struct FileList
        {
            public string filePath;
            public string fileName;
            public long fileSize;
            public string origFileName;

            public override string ToString()
            {
                return filePath + fileName;
            }
        }
        public static void ProcessZipFiles()
        {
            string sSQL = "";
            DateTime startTime = DateTime.Now;
            List<Program.FileList> files = new List<Program.FileList>();
            
            try
            {
                // Open database connection
                using (SqlConnection connection = new SqlConnection(Properties.Settings.Default.DatabaseConnectionString))
                {
                    connection.Open();

                    // Get files ready to be unzipped
                    sSQL = "SELECT file_transfer_task_id, file_transfer_task.file_transfer_source_id, file_transfer_source.source_name, file_transfer_task.source_address," + Environment.NewLine;
                    sSQL += "file_transfer_task.destination_address, source_certificate_path, source_password, destination_directory, destination_filename" + Environment.NewLine;
                    sSQL += "FROM etl.file_transfer_task WITH (NOLOCK), etl.file_transfer_source" + Environment.NewLine;
                    sSQL += "WHERE download_status_cd IN ('FTTD_DOWNLOADED_ZIP')" + Environment.NewLine;
                    sSQL += "AND file_transfer_source.file_transfer_source_id = file_transfer_task.file_transfer_source_id" + Environment.NewLine;
                    using (SqlCommand cmd = new SqlCommand(sSQL, connection))
                    {
                        using (SqlDataReader dr = cmd.ExecuteReader())
                        {
                            while (dr.Read())
                            {

                                files = Program.UnzipFile(dr["destination_address"].ToString() + dr["destination_filename"].ToString(), dr["destination_directory"].ToString(), dr["source_name"].ToString(), Convert.ToInt64(dr["file_transfer_task_id"]));
                                //SqlTransaction tran = connection.BeginTransaction();

                                foreach (Program.FileList i in files)
                                {
                                    sSQL = "INSERT INTO etl.file_transfer_task" + Environment.NewLine;
                                    sSQL += "(file_transfer_source_id, download_status_cd, source_address, source_filename, destination_address, destination_filename," + Environment.NewLine;
                                    sSQL += "file_size_bytes, machine_name, start_date, stop_date)" + Environment.NewLine;
                                    sSQL += "VALUES (" + dr["file_transfer_source_id"].ToString() + "," + Environment.NewLine;
                                    sSQL += "'FTTD_DOWNLOADED', " + Program.IsEmptyWithQuotes(dr["source_address"].ToString()) + "," + Environment.NewLine;
                                    sSQL += Program.IsEmptyWithQuotes(i.origFileName) + "," + Program.IsEmptyWithQuotes(i.filePath) + "," + Environment.NewLine;
                                    sSQL += Program.IsEmptyWithQuotes(i.fileName) + "," + i.fileSize + ", '" + Environment.MachineName + "', getdate(), getdate())" + Environment.NewLine;
                                    using (SqlCommand cmdInsert = new SqlCommand(sSQL, connection))
                                    {
                                        long result = Convert.ToInt64(cmdInsert.ExecuteScalar());
                                    }
                                }
                                //tran.Commit();
                                Program.UpdateTaskStatus(Convert.ToInt64(dr["file_transfer_task_id"]), downloadStatusCode: "FTTD_DOWNLOADED", loadStatusCode: "FTTL_UNZIPPED");
                            }
                        }
                    }
                }
            }
            catch (Exception ex)
            {
                Console.WriteLine("Error:" + ex.Message);
                Program.LogError(Properties.Settings.Default.TaskName + ":UnzipFile", " ", ex);
            }
        }
        public static void SendAttachmentViaEmail(string messageRecipient, string messageSubject, string messageBody, string filePathname)
        {
            try
            {
                MailMessage mail = new MailMessage();
                SmtpClient SmtpServer = new SmtpClient("10.0.0.71");
                mail.From = new MailAddress("eddie.marx@mp2energy.com","Settlement Database");
                mail.To.Add(messageRecipient);
                mail.Subject = messageSubject;
                mail.Body = messageBody;

                System.Net.Mail.Attachment attachment;
                attachment = new System.Net.Mail.Attachment(filePathname);
                mail.Attachments.Add(attachment);

                //SmtpServer.Credentials = new System.Net.NetworkCredential("eddie.marx@mp2energy.com", "password", "M2SQ");
                //SmtpServer.DeliveryMethod = SmtpDeliveryMethod.Network;
                //SmtpServer.Credentials = System.Net.CredentialCache.DefaultNetworkCredentials;
                //SmtpServer.UseDefaultCredentials = true;

                SmtpServer.Send(mail);
            }
            catch (Exception ex)
            {
                Console.WriteLine(ex);
            }
        }
    }
}

       