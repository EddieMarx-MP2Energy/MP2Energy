using System;
using System.Collections.Generic;
using System.Text.RegularExpressions;
using System.Text;
using System.ComponentModel;
using System.IO;
using System.Net;
using System.Data.SqlClient;
using System.Net.Security;
using System.Security.Cryptography.X509Certificates;

namespace SettlementLoader
{
   public class DownloadManager
    {
        public bool DownloadFile(string sourceUrl, string targetFolder, long fileTransferTaskID, string sourceName, string certPath = "", string certPassword = "")
        {
            string tempFilename = sourceName + "_" + fileTransferTaskID + "_downloading.file";  // must specificy filename before download starts, use temp filename
            DateTime startTime = DateTime.Now;

            try
            {
                using (MyWebClient downloader = new MyWebClient())
                {
                    downloader.Headers.Add("user-agent", "Mozilla/4.0 (compatible; MSIE 6.0; Windows NT 5.2; .NET CLR 1.0.3705;)");
                    if (certPath != "")
                    {   // Create a collection object and populate it using the PFX file
                        if (downloader.Certificate == null)
                            // for ercot and certificate needed
                        {   X509Certificate2Collection collection = new X509Certificate2Collection();
                            collection.Import(certPath, certPassword, X509KeyStorageFlags.PersistKeySet);
                            downloader.Certificate = collection[0];
                            // http://stackoverflow.com/questions/2859790/the-request-was-aborted-could-not-create-ssl-tls-secure-channel
                            ServicePointManager.Expect100Continue = true;
                            ServicePointManager.SecurityProtocol = SecurityProtocolType.Ssl3;
                            //ServicePointManager.DefaultConnectionLimit = 9999;
                            //ServicePointManager.SecurityProtocol = SecurityProtocolType.Tls | SecurityProtocolType.Tls11 | SecurityProtocolType.Tls12 | SecurityProtocolType.Ssl3;
                            ServicePointManager.ServerCertificateValidationCallback += new RemoteCertificateValidationCallback(AlwaysGoodCertificate);
                        }
                    }
                    else
                    {
                        // for pjm and no certificate needed
                        ServicePointManager.SecurityProtocol = SecurityProtocolType.Tls12;
                        ServicePointManager.Expect100Continue = true;
                    }
                    downloader.UseDefaultCredentials = false;
                    downloader.DownloadFile(new Uri(sourceUrl), targetFolder + tempFilename);

                    string header_contentDisposition = downloader.ResponseHeaders["content-disposition"];
                    string filename;

                    if (String.IsNullOrEmpty(header_contentDisposition))
                    {
                        filename = "null.csv";
                    }
                    else
                    {
                        if (header_contentDisposition.Contains("("))
                        {
                            filename = header_contentDisposition.Substring(header_contentDisposition.IndexOf("filename=") + 9);
                        }
                        else
                        {
                            filename = new System.Net.Mime.ContentDisposition(header_contentDisposition).FileName;
                        }
                    }
                    string destinationFilename = sourceName + "_" + fileTransferTaskID + "_" + filename;
                    //long fileSize = new System.Net.Mime.ContentDisposition(header_contentDisposition).Size;  // this didn't work, returns -1
                    long fileSize = new System.IO.FileInfo(targetFolder + tempFilename).Length;

                    File.Delete(targetFolder + destinationFilename);
                    var sources = new List<string>() { "monthlybillingstatement", "weeklybillingstatement" };
                    if ((sources.Contains(sourceName)) && fileSize <= (25 * 1024))
                    {
                        Console.WriteLine("Skipped PJM PDF Statement due to size:" + destinationFilename);
                        File.Move(targetFolder + tempFilename, targetFolder + destinationFilename + ".delete");
                        File.Delete(targetFolder + destinationFilename);
                        Program.UpdateTaskStatus(fileTransferTaskID, downloadStatusCode: "FTTD_DOWNLOADED", loadStatusCode: "FTTD_SKIP", sourceFileName: filename, destinationFileName: destinationFilename, fileSize: fileSize);
                    }
                    else
                    {
                        File.Move(targetFolder + tempFilename, targetFolder + destinationFilename);     // actual filename is not known until after the download starts

                        if (destinationFilename.ToLower().Contains(".zip"))
                        {

                            Console.WriteLine("Download zipped file completed:" + destinationFilename);
                            Program.UpdateTaskStatus(fileTransferTaskID, downloadStatusCode: "FTTD_DOWNLOADED_ZIP", sourceFileName: filename, destinationFileName: destinationFilename, fileSize: fileSize);
                        }
                        else
                        {

                            Console.WriteLine("Download Completed:" + destinationFilename);
                            Program.UpdateTaskStatus(fileTransferTaskID, downloadStatusCode: "FTTD_DOWNLOADED", sourceFileName: filename, destinationFileName: destinationFilename, fileSize: fileSize);
                        }
                    }
                    Program.LogSession(Properties.Settings.Default.TaskName + ":DownloadFile", destinationFilename, startTime);
                    return true;
                }
            }
            catch (Exception ex)
            {
                Console.WriteLine("Error:" + ex.Message);
                Program.LogError(Properties.Settings.Default.TaskName + ":DownloadFile", tempFilename, ex);
                return false;
            }
        }

        private static bool AlwaysGoodCertificate(object sender, X509Certificate certificate, X509Chain chain, SslPolicyErrors policyErrors)
        {
            return true;
        }

        public static void ProcessDownloads()
        {
            string sSQL="";
            DateTime startTime = DateTime.Now;

            // Open database connection
            try
            {
                using (SqlConnection connection = new SqlConnection(Properties.Settings.Default.DatabaseConnectionString))
                {
                    connection.Open();

                    // Process download queue records
                    sSQL = "SELECT file_transfer_task.file_transfer_task_id, file_transfer_source.source_name, file_transfer_task.source_address," + Environment.NewLine;
                    sSQL += "file_transfer_task.destination_address, source_certificate_path, source_password" + Environment.NewLine;
                    sSQL += "FROM etl.file_transfer_task, etl.file_transfer_source" + Environment.NewLine;
                    sSQL += "with (nolock)" + Environment.NewLine;
                    sSQL += "WHERE download_status_cd IN ('FTTD_DOWNLOAD_QUEUED', 'FTTD_RETRY')" + Environment.NewLine;
                    sSQL += "AND file_transfer_source.status_cd = 'FTS_READY'" + Environment.NewLine;  // TEMPORARY
                    sSQL += "AND file_transfer_source.file_transfer_source_id = file_transfer_task.file_transfer_source_id" + Environment.NewLine;

                    using (SqlCommand cmd = new SqlCommand(sSQL, connection))
                    {
                        using (SqlDataReader dr = cmd.ExecuteReader())
                        {
                            while (dr.Read())
                            {
                                Program.UpdateTaskStatus(Convert.ToInt64(dr["file_transfer_task_id"]), downloadStatusCode: "FTTD_DOWNLOADING");

                                DownloadManager downloadManager = new DownloadManager();
                                bool result = downloadManager.DownloadFile(dr["source_address"].ToString(), dr["destination_address"].ToString(), Convert.ToInt64(dr["file_transfer_task_id"]), dr["source_name"].ToString(), dr["source_certificate_path"].ToString(), dr["source_password"].ToString());
                                if (result == false)    // success got updated in the called procedure where other metadata was available to log
                                {
                                    Program.UpdateTaskStatus(Convert.ToInt64(dr["file_transfer_task_id"]), downloadStatusCode: "FTTD_ERROR");
                                }

                            }
                            Program.LogSession(Properties.Settings.Default.TaskName + ":ProcessDownloads", "Processed " + dr.RecordsAffected + " task records.", startTime);
                        }
                    }
                }

            }
            catch (Exception ex)
            {
                Console.WriteLine("Error:" + ex.Message);
                Program.LogError(Properties.Settings.Default.TaskName + ":ProcessDownloads", sSQL, ex);
            }

        }
        public static void CreateTransferTasksMSRS()
        {
            string sSQL = "";
            DateTime startTime = DateTime.Now;

            // Open database connection
            try
            {
                    using (SqlConnection connection = new SqlConnection(Properties.Settings.Default.DatabaseConnectionString))
                    {
                        connection.Open();

                        // Create file_transfer_task queue records
                        using (SqlCommand cmd = new SqlCommand("etl.sp_prepare_file_transfer_tasks", connection))
                        {
                            cmd.CommandType = System.Data.CommandType.StoredProcedure;
                            int result = cmd.ExecuteNonQuery();
                            Console.WriteLine("sp_prepare_file_transfer_tasks created " + result + " queue records.");
                            Program.LogSession(Properties.Settings.Default.TaskName + ":ProcessDownloads", "sp_prepare_file_transfer_tasks created " + result + " queue records.", startTime);
                        }
                    }
            }
            catch (Exception ex)
            {
                Console.WriteLine("Error:" + ex.Message);
                Program.LogError(Properties.Settings.Default.TaskName + ":ProcessDownloads", sSQL, ex);
            }
        }
        public static void CreateTransferTasksERCOT()
        {
            string sSQL = "";
            DateTime startTime = DateTime.Now;
            
            // Open database connection
            try
            {
                using (SqlConnection connection = new SqlConnection(Properties.Settings.Default.DatabaseConnectionString))
                {
                    connection.Open();

                    // Get ERCOT file sources to check for new files
                    sSQL = "select file_transfer_source_id, source_name, source_address, source_certificate_path," + Environment.NewLine;
                    sSQL += "    [source_directory], destination_address, utility_name, source_password" + Environment.NewLine;
                    sSQL += "from etl.file_transfer_source" + Environment.NewLine;
                    //sSQL += "with (nolock)" + Environment.NewLine;
                    //sSQL += "where status_cd = 'FTS_READY'" + Environment.NewLine;
                    sSQL += "where status_cd = 'FTS_READY'" + Environment.NewLine;    // TEMPORARY
                    sSQL += "    AND transfer_method_cd IN ('TM_ERCOT_MIS_HTTP', 'TM_ERCOT_MIS_HTTP_ST', 'TM_ERCOT_HTTP_LOSS', 'TM_ERCOT_HTTP_PROFILE')" + Environment.NewLine;
                    using (SqlCommand cmd = new SqlCommand(sSQL, connection))
                    {
                        using (SqlDataReader dr = cmd.ExecuteReader())
                        {
                            while (dr.Read())       // for each file source... (one file source per ercot digital certificate and per report)
                            {
                                List<LinkItem> files;
                                files = GetDirectory(dr["source_address"].ToString(), dr["source_certificate_path"].ToString(), dr["source_password"].ToString());

                                foreach (LinkItem i in files)   //  for each file in the MIS directory for specified certificate and for specified report
                                {
                                    using (SqlCommand cmdInsert = new SqlCommand("etl.sp_prepare_file_transfer_tasks_ercot", connection))
                                    {
                                        cmdInsert.CommandType = System.Data.CommandType.StoredProcedure;
                                        cmdInsert.Parameters.Add(new SqlParameter("source_address", i.Href));
                                        cmdInsert.Parameters.Add(new SqlParameter("source_name", dr["source_name"]));
                                        cmdInsert.Parameters.Add(new SqlParameter("utility_name", dr["utility_name"]));
                                        int result = cmdInsert.ExecuteNonQuery();
                                        Console.WriteLine("sp_prepare_file_transfer_tasks_ercot created " + result + " queue records:" + i.Href);
                                        Program.LogSession(Properties.Settings.Default.TaskName + ":CreateTransferTasksERCOT", "sp_prepare_file_transfer_tasks_ercot created " + result + " queue records:" + i.Href, startTime);
                                    }
                                }
                            }
                        }
                    }
                }
            }
            catch (Exception ex)
            {
                Console.WriteLine("Error:" + ex.Message);
                Program.LogError(Properties.Settings.Default.TaskName + ":ProcessDownloads", sSQL, ex);
            }
        }
        private static List<LinkItem> GetDirectory (string url, string certPath, string certPassword)
        {
            //  Create a collection object and populate it using the PFX file
            //  http://stackoverflow.com/questions/5036590/how-to-retrieve-certificates-from-a-pfx-file-with-c
            //  http://stackoverflow.com/questions/15926142/regular-expression-for-finding-href-value-of-a-a-link/34039184#34039184
               
            MyWebClient client = new MyWebClient();

            if (certPath != "")
            {   // for ercot and digital certificate is required
                X509Certificate2Collection collection = new X509Certificate2Collection();
                collection.Import(certPath, certPassword, X509KeyStorageFlags.PersistKeySet);
                client.Certificate = collection[0];
                ServicePointManager.SecurityProtocol = SecurityProtocolType.Ssl3;
                ServicePointManager.Expect100Continue = true;
            }
            else
            {   // for pjm with no digital certificate
                client.Certificate = null;
                ServicePointManager.SecurityProtocol = SecurityProtocolType.Tls12;
            }

            // download the file listing html for this particular extract/report
            string page = client.DownloadString(url);

            Match m;
            string HRefPattern = "href\\s*=\\s*(?:[\"'](?<1>[^\"']*)[\"']|(?<1>\\S+))";
            List<LinkItem> list = new List<LinkItem>();

            try
            {   // parse directory listing html and locate links
                m = Regex.Match(page, HRefPattern, RegexOptions.IgnoreCase | RegexOptions.Compiled, TimeSpan.FromSeconds(10));

                while (m.Success) 
                {
                    if (m.Groups[1].Value.Contains("mirDownload"))
                    {   // directory listing has two links per file, one for display and one for download
                        LinkItem i = new LinkItem();
                        i.Href = Properties.Settings.Default.BaseURLErcot + m.Groups[1].Value;
                       list.Add(i);
                    }
                    m = m.NextMatch();
                }
                return list;
            }
            catch (RegexMatchTimeoutException)
            {
                Console.WriteLine("The matching operation timed out.");
                return list;
            }
        }
   }
    public class MyWebClient : WebClient
    {
        public X509Certificate2 Certificate { get; set; }
        public readonly CookieContainer CookieContainer = new CookieContainer();
        protected override WebRequest GetWebRequest(Uri address)
        {
            var request = (HttpWebRequest)base.GetWebRequest(address);

            request.CookieContainer = CookieContainer;

            if (Certificate != null) request.ClientCertificates.Add(Certificate);

            return request;
        }
    }
    public struct LinkItem
    {
        public string Href;
        public string Text;

        public override string ToString()
        {
            return Href + "\n\t" + Text;
        }
    }
}

