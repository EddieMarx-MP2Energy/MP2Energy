using System;
using System.Collections.Generic;
using System.Text;
using System.ComponentModel;
using System.IO;
using System.Net;
using System.Data.SqlClient;

namespace SettlementLoader
{
   public class DownloadManager
    {
        public bool DownloadFile(string sourceUrl, string targetFolder, long fileTransferTaskID, string sourceName)
        {
            string tempFilename = sourceName + "_" + fileTransferTaskID + "_downloading.file";  // must specificy filename before download starts, use temp filename
            DateTime startTime = DateTime.Now;

            try
            {
                using (WebClient downloader = new WebClient())
                {
                    downloader.DownloadFile(new Uri(sourceUrl), targetFolder + tempFilename);

                    string header_contentDisposition = downloader.ResponseHeaders["content-disposition"];
                    string filename = new System.Net.Mime.ContentDisposition(header_contentDisposition).FileName;
                    string destinationFilename = sourceName + "_" + fileTransferTaskID + "_" + filename;
                    //long fileSize = new System.Net.Mime.ContentDisposition(header_contentDisposition).Size;  // this didn't work, returns -1
                    long fileSize = new System.IO.FileInfo(targetFolder + tempFilename).Length;

                    File.Move(targetFolder + tempFilename, targetFolder + destinationFilename);     // actual filename is not known until after the download starts

                    Console.WriteLine("Download Completed:" + destinationFilename);
                    Program.UpdateTaskStatus(fileTransferTaskID, downloadStatusCode: "FTTD_DOWNLOADED", sourceFileName: filename, destinationFileName: destinationFilename, fileSize: fileSize);
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

        public static void ProcessDownloads()
        {
            string sSQL="";
            DateTime startTimeOverall = DateTime.Now;

            // Open database connection
            try
            {
                using (var connection = new SqlConnection(Properties.Settings.Default.DatabaseConnectionString))
                {
                    connection.Open();

                    // Create file_transfer_task queue records
                    using (var cmd = new SqlCommand("etl.sp_prepare_file_transfer_tasks", connection))
                    {
                        DateTime startTime = DateTime.Now;

                        cmd.CommandType = System.Data.CommandType.StoredProcedure;
                        int result = cmd.ExecuteNonQuery();
                        Console.WriteLine("sp_prepare_file_transfer_tasks created " + result + " queue records.");
                        Program.LogSession(Properties.Settings.Default.TaskName + ":ProcessDownloads", "sp_prepare_file_transfer_tasks created " + result + " queue records.", startTime);
                    }

                    // Process download queue records
                    using (SqlCommand cmd = new SqlCommand())
                    {
                        DateTime startTime = DateTime.Now;

                        sSQL = "SELECT file_transfer_task.file_transfer_task_id, file_transfer_source.source_name, file_transfer_task.source_address, file_transfer_task.destination_address" + Environment.NewLine;
                        sSQL += "FROM etl.file_transfer_task, etl.file_transfer_source" + Environment.NewLine;
                        sSQL += "WHERE download_status_cd IN ('FTTD_DOWNLOAD_QUEUED', 'FTTD_RETRY')" + Environment.NewLine;
                        sSQL += "AND file_transfer_source.file_transfer_source_id = file_transfer_task.file_transfer_source_id" + Environment.NewLine;

                        cmd.Connection = connection;
                        cmd.CommandText = sSQL;

                        using (SqlDataReader dr = cmd.ExecuteReader())
                        {
                            while (dr.Read())
                            {
                                Program.UpdateTaskStatus(Convert.ToInt64(dr["file_transfer_task_id"]), downloadStatusCode: "FTTD_DOWNLOADING");

                                DownloadManager downloadManager = new DownloadManager();
                                bool result = downloadManager.DownloadFile(dr["source_address"].ToString(), dr["destination_address"].ToString(), Convert.ToInt64(dr["file_transfer_task_id"]), dr["source_name"].ToString());
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
    }
}
