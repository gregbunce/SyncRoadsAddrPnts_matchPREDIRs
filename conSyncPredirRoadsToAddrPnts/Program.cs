using System;
using System.Collections.Generic;
using System.Linq;
using System.Text;
using System.Threading.Tasks;
using System.IO;
using System.Data;
using System.Data.SqlClient;
using System.Configuration;

namespace conSyncPredirRoadsToAddrPnts
{
    class Program
    {
        static void Main(string[] args)
        {
            try
            {
                // get access to the date and time for the text file name
                string strYearMonthDayHourMin = DateTime.Now.ToString("-yyyy-MM-dd-HH-mm");

                // create sql query string for recordset to loop through
                string strSqlQuery = @"select top(5) * from UTRANS_STREETS
                                        where CARTOCODE not in ('1','7','99')
                                        and (HWYNAME = '')
                                        and ((L_F_ADD <> 0 and L_T_ADD <> 0) OR (R_F_ADD <> 0 and R_T_ADD <> 0))
                                        and (STREETNAME like '%[A-Z]%')
                                        and (STREETNAME <> '')
                                        and (PREDIR = '')
                                        and (STREETNAME not like '%ROUNDABOUT%')
                                        and (STREETNAME not like '% SB')
                                        and (STREETNAME not like '% NB')
                                        order by STREETNAME";

                //setup a file stream and a stream writer to write out the road segments that get predirs from address points
                string path = @"C:\temp\SyncPREDIRs" + strYearMonthDayHourMin + ".txt";
                FileStream fileStream = new FileStream(path, FileMode.Create);
                StreamWriter streamWriter = new StreamWriter(fileStream);
                // write the first line of the text file - this is the field headings
                streamWriter.WriteLine("ITTR_ID" + "," + "UNIQUE_ID" + "," + "STREETNAME" + "," + "STREETTYPE" + "," + "MATCH_DISTANCE" + "," + "MATCH_UNIQUEID" + "," + "MATCH_PREDIR" + "," + "MATCH_FULLADD");
                int intIttrID = 0;

                // get connection string to sql database from appconfig
                var connectionString = ConfigurationManager.AppSettings["myConn"];

                // get a record set of road segments that need assigned predirs 
                using (SqlConnection con1 = new SqlConnection(connectionString))
                {
                    // open the sqlconnection
                    con1.Open();

                    // create a sqlcommand - allowing for a subset of records from the table
                    using (SqlCommand command1 = new SqlCommand(strSqlQuery, con1))

                    // create a sqldatareader
                    using (SqlDataReader reader1 = command1.ExecuteReader())
                    {
                        if (reader1.HasRows)
                        {
                            // loop through the record set
                            while (reader1.Read())
                            {
                                // itterate the row count
                                intIttrID = intIttrID + 1;

                                // get the current road segments oid
                                int intRoadOID = Convert.ToInt32(reader1["OBJECTID"]);
                                string strUtransUniqueID = reader1["UNIQUE_ID"].ToString();
                                string strStreetName = reader1["STREETNAME"].ToString();
                                string strStreetType = reader1["STREETTYPE"].ToString();
                                string strStreetSufDir = reader1["SUFDIR"].ToString();

                                // get the centroid of the current road segment - used to query the nearest address point
                                //string strRoadCentroid = GetRoadSegmentCentroid(intRoadOID);

                                // get the address point nearest to the current road segment's centroid, that has the same streetname and streettype
                                // tuple item1=fulladdress; item2=matchdistance; item3=predir; item4=uniqueid
                                Tuple<string, int, string, string> tplNearestAddress = GetNearestMatchingAddressPnt(intRoadOID, strStreetName, strStreetSufDir, strStreetType);
                                
                                // check the distance to the nearest found address point and make sure it's reasonable (maybe 250 meters?)
                                if (tplNearestAddress.Item2 < 250)
                                {
                                    streamWriter.WriteLine(intIttrID + "," + strUtransUniqueID + "," + strStreetName + "," + strStreetType + "," + tplNearestAddress.Item2 + "," + tplNearestAddress.Item4 + "," + tplNearestAddress.Item3 + "," + tplNearestAddress.Item1);
                                }
                                else // maybe do something different with these records that are outside of our distance tolerance, flag them differently (?) 
                                {
                                    streamWriter.WriteLine(intIttrID + "," + strUtransUniqueID + "," + strStreetName + "," + strStreetType + "," + tplNearestAddress.Item2 + "," + tplNearestAddress.Item4 + "," + tplNearestAddress.Item3 + "," + tplNearestAddress.Item1);
                                }
                            }
                        }
                    }
                }

                //close the stream writer
                streamWriter.Close();
            }
            catch (Exception ex)
            {
                Console.WriteLine("There was an error with the conSyncPredirRoadsToAddrPnts console application, in the Main method." + ex.Message + " " + ex.Source + " " + ex.InnerException + " " + ex.HResult + " " + ex.StackTrace + " " + ex);
                Console.ReadLine();
            }
        }



        // this method gets the road segment's centroid
        static string GetRoadSegmentCentroid(int intOID)
        {
            try
            {
                string strCentroidCordPair = string.Empty;
                string strQueryStringCentroid = @"DECLARE @gg geometry = (SELECT SHAPE.STBuffer(1) as buffer from UTRANS_STREETS where OBJECTID =" + intOID + @");
	                                            DECLARE @CentroidUtrans geometry = (SELECT @gg.STCentroid().ToString() as myCentroid);
	                                            SELECT @CentroidUtrans.ToString() as MyCentroid;";

                // get connection string to sql database from appconfig
                var connectionString = ConfigurationManager.AppSettings["myConn"];

                // get a record set of road segments that need assigned predirs 
                using (SqlConnection con2 = new SqlConnection(connectionString))
                {
                    // open the sqlconnection
                    con2.Open();

                    // create a sqlcommand - allowing for a subset of records from the table
                    using (SqlCommand command2 = new SqlCommand(strQueryStringCentroid, con2))

                    // create a sqldatareader
                    using (SqlDataReader reader2 = command2.ExecuteReader())
                    {
                        if (reader2.HasRows)
                        {
                            // loop through the record set
                            while (reader2.Read())
                            {
                                // pass the centrod cordinate pair back to the Main method
                                strCentroidCordPair = reader2["MyCentroid"].ToString();
                            }
                        }
                        else
                        {
                            strCentroidCordPair = "-1";
                        }
                    }
                }
                return strCentroidCordPair;
            }
            catch (Exception ex)
            {
                Console.WriteLine("There was an error with the conSyncPredirRoadsToAddrPnts console application, in the GetRoadSegmentCentroid method." + ex.Message + " " + ex.Source + " " + ex.InnerException + " " + ex.HResult + " " + ex.StackTrace + " " + ex);
                Console.ReadLine();
                return "-1";
            }
        }



        // this method gets the address point nearest to specified road segment's centroid, that has the same streetname and streettype
        static Tuple<string, int, string, string> GetNearestMatchingAddressPnt(int intOID, string strStName, string strStSufx, string strStType)
        {
            try
            {
                string strNearestMatchFullAddr = string.Empty;
                int strNearestMatchDist = 0;
                string strNearestMatchPreDir = string.Empty;
                string strNearestMatchUniqueID = string.Empty;

                string strQueryStringNearMatchAddr = @"DECLARE @g geometry = (select Shape from UTRANS_STREETS where OBJECTID = " + intOID + @");
                                                    SELECT TOP(1) Shape.STDistance(@g) as distance, ADDRESSPOINTS.FullAdd, ADDRESSPOINTS.PrefixDir, ADDRESSPOINTS.UTAddPtID FROM ADDRESSPOINTS
                                                    WHERE Shape.STDistance(@g) IS NOT NULL and ADDRESSPOINTS.StreetName = '" + strStName + @"' and ADDRESSPOINTS.StreetType = '" + strStType + @"' and ADDRESSPOINTS.SuffixDir = '" + strStSufx + @"'
                                                    ORDER BY Shape.STDistance(@g);";

                // get connection string to sql database from appconfig
                var connectionString = ConfigurationManager.AppSettings["myConn"];

                // get a record set of road segments that need assigned predirs 
                using (SqlConnection con3 = new SqlConnection(connectionString))
                {
                    // open the sqlconnection
                    con3.Open();

                    // create a sqlcommand - allowing for a subset of records from the table
                    using (SqlCommand command3 = new SqlCommand(strQueryStringNearMatchAddr, con3))

                    // create a sqldatareader
                    using (SqlDataReader reader3 = command3.ExecuteReader())
                    {
                        if (reader3.HasRows)
                        {
                            // loop through the record set
                            while (reader3.Read())
                            {
                                // pass the centrod cordinate pair back to the Main method
                                strNearestMatchFullAddr = reader3["FullAdd"].ToString();
                                strNearestMatchDist = Convert.ToInt32(reader3["distance"]);
                                strNearestMatchPreDir = reader3["PrefixDir"].ToString();
                                strNearestMatchUniqueID = reader3["UTAddPtID"].ToString();
                            }
                        }
                        else
                        {
                            strNearestMatchFullAddr = "-1";
                        }
                    }
                }
                return Tuple.Create(strNearestMatchFullAddr, strNearestMatchDist, strNearestMatchPreDir, strNearestMatchUniqueID);
            }
            catch (Exception ex)
            {
                Console.WriteLine("There was an error with the conSyncPredirRoadsToAddrPnts console application, in the GetNearestMatchingAddressPnt method." + ex.Message + " " + ex.Source + " " + ex.InnerException + " " + ex.HResult + " " + ex.StackTrace + " " + ex);
                Console.ReadLine();
                return Tuple.Create("-1", -1, "-1", "-1");
            }
        }



    }

}
