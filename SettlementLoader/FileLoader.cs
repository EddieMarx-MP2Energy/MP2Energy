using Newtonsoft.Json;
using System;
using System.Collections.Generic;
using System.Linq;
using System.Text;
using System.Threading.Tasks;
using System.Data.SqlClient;
using System.IO;
using System.Xml;
using System.Web;

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
                    sSQL += "with (nolock)" + Environment.NewLine;
                    sSQL += "WHERE download_status_cd IN ('FTTD_DOWNLOADED')" + Environment.NewLine;
                    sSQL += "    AND status_cd = 'FTS_DEV'";  // TEMPORARY
                    sSQL += "    AND file_transfer_source.status_cd = 'FTS_DEV'" + Environment.NewLine;  // TEMPORARY
                    sSQL += "    AND transfer_method_cd IN ('TM_MSRS_PDF_HTTP', 'TM_JSON_LMP', 'TM_MSRS_HTTP', 'TM_MSRS_BILL_HTTP', 'TM_INSCHEDULES', 'TM_ERCOT_MIS_HTTP', 'TM_ERCOT_MIS_HTTP_ST')" + Environment.NewLine;  
                    sSQL += "    AND file_transfer_source.file_transfer_source_id = file_transfer_task.file_transfer_source_id" + Environment.NewLine;
                    sSQL += "    AND (load_status_cd IS NULL" + Environment.NewLine;
                    sSQL += "        OR load_status_cd IN ('FTTL_READY', 'FTTL_RETRY', 'FTTL_STATUS_NEW'))" + Environment.NewLine;
                    //sSQL += "    AND source_name like '%prde%'"; // TEMPORARY
                    sSQL += "ORDER BY stop_date, file_transfer_task.source_filename" + Environment.NewLine;  // must load HEADERS before INTERVAL/STATUS, just happens to be in alphabetical order
                    using (SqlCommand cmd = new SqlCommand(sSQL, connection))
                    {
                        using (SqlDataReader dr = cmd.ExecuteReader())
                        {
                            while (dr.Read())
                            {
                                Program.UpdateTaskStatus(Convert.ToInt64(dr["file_transfer_task_id"]), loadStatusCode: "FTTL_LOADING");

                                bool result = false;

                                if (dr["transfer_method_cd"].ToString() == "TM_MSRS_HTTP")
                                {
                                    FileLoader fileloader = new FileLoader();
                                    result = fileloader.LoadFileMSRSReport(Convert.ToInt64(dr["file_transfer_task_id"]), dr["destination_address"].ToString() + dr["destination_filename"].ToString(), dr["source_name"].ToString());
                                }
                                else if (dr["transfer_method_cd"].ToString() == "TM_MSRS_BILL_HTTP")
                                {
                                    FileLoader fileloader = new FileLoader();
                                    result = fileloader.LoadFileMSRSBill(Convert.ToInt64(dr["file_transfer_task_id"]), dr["destination_address"].ToString() + dr["destination_filename"].ToString());
                                }
                                else if (dr["transfer_method_cd"].ToString() == "TM_INSCHEDULES")
                                {
                                    FileLoader fileloader = new FileLoader();
                                    result = fileloader.LoadFileInSchedulesLosses(Convert.ToInt64(dr["file_transfer_task_id"]), dr["destination_address"].ToString() + dr["destination_filename"].ToString());
                                }
                                else if (dr["transfer_method_cd"].ToString() == "TM_ERCOT_MIS_HTTP")
                                {
                                    if (dr["destination_filename"].ToString().Substring(dr["destination_filename"].ToString().Length - 3).ToLower() == "xml")

                                    {
                                        FileLoader fileloader = new FileLoader();
                                        result = fileloader.LoadFileERCOTExtract(Convert.ToInt64(dr["file_transfer_task_id"]), dr["destination_address"].ToString() + dr["destination_filename"].ToString());
                                    }
                                    else
                                    {
                                        result = true;  // skip CSV files.  TODO:  do this better so that "true" is not returned and the file marked as skipped
                                        Console.WriteLine("skipping CSV file");
                                    }
                                }
                                else if (dr["transfer_method_cd"].ToString() == "TM_ERCOT_MIS_HTTP_ST")
                                {
                                    if (dr["destination_filename"].ToString().Substring(dr["destination_filename"].ToString().Length - 3).ToLower() == "xml")

                                    {
                                        FileLoader fileloader = new FileLoader();
                                        result = fileloader.LoadFileERCOTStatement(Convert.ToInt64(dr["file_transfer_task_id"]), dr["destination_address"].ToString() + dr["destination_filename"].ToString());
                                    }
                                    else
                                    {
                                        result = true;  // skip CSV files.  TODO:  do this better so that "true" is not returned and the file marked as skipped
                                        Console.WriteLine
                                            ("skipping statement CSV file");
                                    }
                                }
                                else if (dr["transfer_method_cd"].ToString() == "TM_JSON_LMP")
                                {
                                    FileLoader fileloader = new FileLoader();
                                    result = fileloader.LoadFileMinerLMP(Convert.ToInt64(dr["file_transfer_task_id"]), dr["destination_address"].ToString() + dr["destination_filename"].ToString());
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
        public bool LoadFileERCOTStatement(long fileTransferTaskID, string pathName)
        {
            string sqlInsert = "";
            string sqlValues = "";
            long result = 0;
            long result3 = 0;
            long resultCharge = 0;
            long resultBillDetail = 0;
            bool hasActivity = false;

            try
            {
                using (SqlConnection connection = new SqlConnection(Properties.Settings.Default.DatabaseConnectionString))
                {
                    connection.Open();

                    XmlDocument xml = new XmlDocument();
                    //xml.Load("\\\\mp2-filesrv1\\wholesale\\OEA\\PJM\\Monthly_Settlements\\ERCOTData\\ercot_rtm_stmt_124268_rpt.00011107.0008281436312200.20161220.091527229.RTM_TRUEUP_STATEMENT_20160623_8281436312200_T3.XML");
                    xml.Load(pathName);
                    XmlNodeList xnList = xml.ChildNodes[0].ChildNodes[0].ChildNodes;
                    sqlInsert = "SET NOCOUNT ON; INSERT INTO etl.ercot_stmt (" + Environment.NewLine + "file_transfer_task_id,";
                    sqlValues = "VALUES (" + fileTransferTaskID + ",";

                    foreach (XmlNode xmlRow in xnList)
                    {
                        sqlInsert += xmlRow.Name + ",";
                        sqlValues += Program.IsEmptyWithQuotes(xmlRow.InnerText) + ",";
                    }

                    // GET TOTALS FROM END OF DOCUMENT
                    //xnList = xml.ChildNodes[0].ChildNodes[2].ChildNodes;
                    xnList = xml.SelectNodes("/Statement/Summary/CurrentDollars");
                    sqlInsert += "StatementTotal, NetAmount)" + Environment.NewLine;
                    sqlValues += xnList[0].ChildNodes[0].InnerText + "," + xnList[0].ChildNodes[1].InnerText + ")" + Environment.NewLine;

                    using (SqlCommand cmd = new SqlCommand(sqlInsert + sqlValues + "; SELECT SCOPE_IDENTITY();", connection))
                    {
                        try
                        {
                            result = Convert.ToInt64(cmd.ExecuteScalar());
                        }
                        catch (Exception ex)
                        {
                            if (ex.Message.Contains("duplicate"))
                            {
                                //Console.WriteLine("duplicate");
                                // only one statement per file
                                // do nothing on unique constraint violations.  TODO:  replace with MERGE statement
                                return true;
                            }
                            else
                            {
                                if (ex.Message.Contains("conflicted"))
                                {
                                    Console.WriteLine("Error:" + ex.Message);
                                    return false;
                                }
                                else
                                {
                                    Console.WriteLine("Error:" + ex.Message);
                                    Program.LogError(Properties.Settings.Default.TaskName + ":LoadFile", sqlInsert + sqlValues, ex);
                                    return false;
                                }
                            }
                        }
                    }

                    xnList = xml.ChildNodes[0].ChildNodes[1].ChildNodes;
                    foreach (XmlNode xmlRow in xnList)
                    {
                        sqlInsert = "SET NOCOUNT ON; INSERT INTO etl.ercot_stmt_charge (" + Environment.NewLine + "ercot_stmt_id,";
                        sqlValues = "VALUES (" + result + ",";
                        hasActivity = false;

                        foreach (XmlNode xmlRow2 in xmlRow)
                        {
                            if (xmlRow2.Name == "BillingDetails")
                            {
                                if (sqlInsert.Contains("etl.ercot_stmt_charge"))
                                {
                                    // SAVE CHARGE LINE ITEMS THAT HAPPENED IN ercot_stmt_charge
                                    sqlInsert = sqlInsert.Substring(0, sqlInsert.Length - 1) + ")" + Environment.NewLine;   // remove extra column separators ","
                                    sqlValues = sqlValues.Substring(0, sqlValues.Length - 1) + ")" + Environment.NewLine;
                                    using (SqlCommand cmdCharge = new SqlCommand(sqlInsert + sqlValues + "; SELECT SCOPE_IDENTITY();", connection))

                                    {
                                        resultCharge = Convert.ToInt64(cmdCharge.ExecuteScalar());
                                    }
                                }
                                sqlInsert = "SET NOCOUNT ON; INSERT INTO etl.ercot_stmt_bill_detail (" + Environment.NewLine + "ercot_stmt_charge_id,";
                                sqlValues = "VALUES (" + resultCharge + ",";

                                hasActivity = false;
                                foreach (XmlNode xmlBillDetail in xmlRow2)
                                {
                                    sqlInsert += xmlBillDetail.Name + ",";
                                    sqlValues += Program.IsEmptyWithQuotes(xmlBillDetail.InnerText) + ",";
                                    if (xmlBillDetail.InnerText == "YES") hasActivity = true;   //TODO: check element name also
                                }

                                sqlInsert = sqlInsert.Substring(0, sqlInsert.Length - 1) + ")" + Environment.NewLine;   // remove extra column separators ","
                                sqlValues = sqlValues.Substring(0, sqlValues.Length - 1) + ")" + Environment.NewLine;
                                using (SqlCommand cmdBillDetail = new SqlCommand(sqlInsert + sqlValues + "; SELECT SCOPE_IDENTITY();", connection))

                                {
                                    resultBillDetail = Convert.ToInt64(cmdBillDetail.ExecuteScalar());
                                }
                            }
                            else if (hasActivity)
                            {
                                // ITERATE THROUGH INTERVALS FOR CHARGE LINE ITEM
                                sqlInsert = "INSERT INTO etl.ercot_stmt_charge_detail (" + Environment.NewLine + "ercot_stmt_charge_id,";
                                sqlValues = "VALUES (" + resultCharge + ",";

                                if (xmlRow2.Name != "NumberOfIntervals")
                                {
                                    foreach (XmlNode xmlCharge in xmlRow2)
                                    {
                                        sqlInsert += xmlCharge.Name + ",";
                                        sqlValues += Program.IsEmptyWithQuotes(xmlCharge.InnerText) + ",";
                                    }

                                    sqlInsert = sqlInsert.Substring(0, sqlInsert.Length - 1) + ")" + Environment.NewLine;   // remove extra column separators ","
                                    sqlValues = sqlValues.Substring(0, sqlValues.Length - 1) + ")" + Environment.NewLine;

                                    using (SqlCommand cmdChargeDetail = new SqlCommand(sqlInsert + sqlValues, connection))
                                    {
                                        result3 = Convert.ToInt64(cmdChargeDetail.ExecuteNonQuery());
                                    }
                                    // break;
                                }

                            }
                            else
                            {
                                sqlInsert += xmlRow2.Name + ",";
                                sqlValues += Program.IsEmptyWithQuotes(xmlRow2.InnerText) + ",";
                            }
                        }
                    }
                }
                return true;
            }
            catch (Exception ex)
            {
                Console.WriteLine("Error:" + ex.Message);
                Program.LogError(Properties.Settings.Default.TaskName + ":LoadFile", sqlInsert + sqlValues, ex);
                return false;
            }
        }
        private bool LoadFileMSRSReport(long fileTransferTaskID, string pathName, string sourceName)
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
                                using (SqlCommand cmd = new SqlCommand(sSQL, connection))
                                {
                                    try
                                    {
                                        int result = cmd.ExecuteNonQuery();
                                    }
                                    catch (Exception ex)
                                    {
                                        if (ex.Message.Contains("duplicate"))
                                        {
                                            //Console.WriteLine("duplicate");
                                            // do nothing on unique constraint violations.  TODO:  replace with MERGE statement
                                        }
                                        else
                                        {
                                            if (ex.Message.Contains("conflicted"))
                                            {
                                                // errors happen here when the DETAIL file is attempted to load before the HEADER files.  On the next go around, maybe the HEADER will have loaded
                                                Console.WriteLine("Error:" + ex.Message);
                                                return false;
                                            }
                                            else
                                            {
                                                Console.WriteLine("Error:" + ex.Message);
                                                Program.LogError(Properties.Settings.Default.TaskName + ":LoadFile", sqlInsert, ex);
                                                return false;
                                            }
                                        }
                                    }
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
            string valuesSQL = ""; //= "VALUES (" + fileTransferTaskID + ", ";
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
        private bool LoadFileMSRSBill(long fileTransferTaskID, string pathName)
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
                            try
                            {
                                billID = Convert.ToInt64(cmd.ExecuteScalar());
                            }
                            catch (Exception ex)
                            {
                                if (ex.Message.Contains("duplicate"))
                                {
                                    tran.Rollback();
                                    //Console.WriteLine("duplicate");
                                    return true;
                                }
                                else
                                {
                                    tran.Rollback();
                                    Console.WriteLine("Error:" + ex.Message);
                                    Program.LogError(Properties.Settings.Default.TaskName + ":LoadFile", sSQL, ex);
                                    return false;
                                }
                            }
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

                                    using (SqlCommand cmd = new SqlCommand(sSQL, connection, tran))
                                    {
                                        try
                                        {
                                            int result = cmd.ExecuteNonQuery();
                                            hadDetail = true;
                                        }
                                        catch (Exception ex)
                                        {
                                            if (ex.Message.Contains("duplicate"))
                                            {
                                                //Console.WriteLine("duplicate");
                                            }
                                            else
                                            {
                                                tran.Rollback();
                                                Console.WriteLine("Error:" + ex.Message);
                                                Program.LogError(Properties.Settings.Default.TaskName + ":LoadFile", sSQL, ex);
                                                return false;
                                            }
                                        }
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
                            using (SqlCommand cmd = new SqlCommand(sSQL, connection, tran))
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
                                { 
                                    if (ex.Message.Contains("duplicate"))
                                    {
                                        //Console.WriteLine("duplicate");
                                        // do nothing on unique constraint violations.  TODO:  replace with MERGE statement
                                    }
                                    else
                                    { //TODO: update the record in the future.}
                                        Console.WriteLine("ERROR:" + ex.Message);
                                        return false;
                                    }
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
        public bool LoadFileERCOTExtract(long fileTransferTaskID, string pathName)
        {
            string sqlInsert = "";
            string sqlValues = "";

            try
            {
                using (SqlConnection connection = new SqlConnection(Properties.Settings.Default.DatabaseConnectionString))
                {
                    connection.Open();

                    XmlDocument xml = new XmlDocument();
                    xml.Load(pathName);
                    XmlNodeList xnList = xml.ChildNodes;
                    foreach (XmlNode xmlRow in xnList[1])
                    {
                        //StringBuilder sb = new StringBuilder();sb.Append(" ");
                        sqlInsert = "INSERT INTO mis." + xnList[1].FirstChild.Name + " (" + Environment.NewLine + "file_transfer_task_id,";
                        sqlValues = "VALUES (" + fileTransferTaskID + ",";
                        foreach (XmlNode xmlCol in xmlRow.ChildNodes)
                        {
                            sqlInsert += xmlCol.Name + ",";
                            sqlValues += Program.IsEmptyWithQuotes(xmlCol.InnerText) + ",";
                        }
                        sqlInsert = sqlInsert.Substring(0, sqlInsert.Length - 1) + ")" + Environment.NewLine;   // remove extra column separators ","
                        sqlValues = sqlValues.Substring(0, sqlValues.Length - 1) + ")" + Environment.NewLine;
                        using (SqlCommand cmd = new SqlCommand(sqlInsert + sqlValues, connection))
                        {
                            try
                            {
                                int result = cmd.ExecuteNonQuery();
                            }
                            catch (Exception ex)
                            {
                                if (ex.Message.Contains("duplicate"))
                                {
                                    //Console.WriteLine("duplicate");
                                    // do nothing on unique constraint violations.  TODO:  replace with MERGE statement
                                }
                                else
                                {
                                    if (ex.Message.Contains("conflicted"))
                                    {
                                        // errors happen here when the DETAIL file is attempted to load before the HEADER files.  On the next go around, maybe the HEADER will have loaded
                                        Console.WriteLine("Error:" + ex.Message);
                                        return false;
                                    }
                                    else
                                    {
                                        Console.WriteLine("Error:" + ex.Message);
                                        Program.LogError(Properties.Settings.Default.TaskName + ":LoadFile", sqlInsert + sqlValues, ex);
                                        return false;
                                    }
                                }
                            }
                        }
                    }
                }
                return true;
            }
            catch (Exception ex)
            {
                Console.WriteLine("Error:" + ex.Message);
                Program.LogError(Properties.Settings.Default.TaskName + ":LoadFile", sqlInsert + sqlValues, ex);
                return false;
            }
        }

        private bool LoadFileMinerLMP(long fileTransferTaskID, string pathName)
        {
            string sSQL = "";

            try
            {
                using (SqlConnection connection = new SqlConnection(Properties.Settings.Default.DatabaseConnectionString))
                {
                    connection.Open();
                    
                    dynamic priceTypes = JsonConvert.DeserializeObject(File.ReadAllText(pathName));

                    for (int l = 0; l < priceTypes.Count; l++)
                    {
                        for (int p = 0; p < priceTypes[l].prices.Count; p++)
                        {
                            sSQL = "INSERT INTO etl.miner_lmp (file_transfer_task_id, publishDate, pnodeId," + Environment.NewLine;
                            sSQL += "versionNum, priceType, utchour, price) VALUES (" + fileTransferTaskID + "," + Environment.NewLine;
                            sSQL += Program.IsEmptyWithQuotes(priceTypes[l].publishDate.ToString()) + "," + Environment.NewLine;
                            sSQL += Program.IsEmptyWithQuotes(priceTypes[l].pnodeId.ToString()) + "," + Environment.NewLine;
                            sSQL += Program.IsEmptyWithQuotes(priceTypes[l].versionNum.ToString()) + "," + Environment.NewLine;
                            sSQL += Program.IsEmptyWithQuotes(priceTypes[l].priceType.ToString()) + "," + Environment.NewLine;
                            sSQL += Program.IsEmptyWithQuotes(priceTypes[l].prices[p].utchour.ToString()) + "," + Environment.NewLine;
                            sSQL += Program.IsEmptyWithQuotes(priceTypes[l].prices[p].price.ToString()) + ")" + Environment.NewLine;
                            try
                            {
                                using (SqlCommand cmd = new SqlCommand(sSQL, connection))
                                {
                                    int result = cmd.ExecuteNonQuery();
                                }
                            }
                            catch (Exception ex)
                            {
                                if (ex.Message.Contains("duplicate"))
                                {
                                    //Console.WriteLine("duplicate");
                                    //return true;
                                    // do nothing on duplicates.  TODO: replace with MERGE
                                }
                                else
                                {
                                    Console.WriteLine("Error:" + ex.Message);
                                    Program.LogError(Properties.Settings.Default.TaskName + ":LoadFileMinerLMP", sSQL, ex);
                                    return false;
                                }
                            }
                        }
                    }
                    return true;
                }
            }
            catch (Exception ex)
            {
                Console.WriteLine("Error:" + ex.Message);
                Program.LogError(Properties.Settings.Default.TaskName + ":LoadFileMinerLMP", sSQL, ex);
                return false;
            }
        }
    }
}
