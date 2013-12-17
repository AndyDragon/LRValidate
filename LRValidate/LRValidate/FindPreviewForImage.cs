//  Copyright (c) LE Ferguson, LLC 2013 
namespace LRValidate
{
    // Summary
    // General purpose routine to pull preview images out of a lightroom catalog.
    // Has very little in common with the other routines in this application, and could be taken for use otherwise
    //
    // To use, instantiate it with the path to the catalog (not the preview database)
    // Then call with GetPreview using an image ID and desired file size to be returned
    using System;
    using System.IO;
    using System.Windows.Media.Imaging;
    
    public class FindPreviewForImage
    {
        private System.Data.Common.DbConnection dbConn; // Connection is shared in threads within a window; different connection in different windows (which are on different threads) 
        private string connString;                      // Main window determines the connection string to use and passes it off
        private string previewDirectory; 

        public FindPreviewForImage(string dbPath /* Path to main catalog NOT preview area */)
        {
            // Initialization routine 
            try
            {
                previewDirectory = Path.GetDirectoryName(dbPath) + @"\" + Path.GetFileNameWithoutExtension(dbPath) + " Previews.lrdata";
                connString = string.Format(@"Data Source={0}; Pooling=false; FailIfMissing=true; ReadOnly=true;", previewDirectory + @"\previews.db");
                using (var factory = new System.Data.SQLite.SQLiteFactory())
                {
                    dbConn = factory.CreateConnection();
                    dbConn.ConnectionString = connString;
                    dbConn.Open();
                } // using factory
            }
            catch (Exception e)
            {   // Having this null will indicate we can't do previews
                dbConn = null; 
            }
        }
        
        public BitmapImage GetPreview(int imageID, int aproximatePixelSize)
        {
            // Access routine - you need to know the Image ID from the catalog
            try
            {
                string path = null;
                string orientation = string.Empty;
                if (dbConn == null)
                {
                    return null; // If we didn't get a database then return no image 
                }
                using (System.Data.Common.DbCommand cmd = dbConn.CreateCommand())
                {
                    cmd.CommandText = @"select uuid || '-' || digest as FileName, orientation from ImageCacheEntry where imageId=" + imageID.ToString();
                    using (System.Data.Common.DbDataReader reader = cmd.ExecuteReader())
                    {
                        if (reader.Read())
                        {
                            path = previewDirectory + @"\" + reader.GetString(reader.GetOrdinal("FileName")).Substring(0, 1) + @"\" + reader.GetString(reader.GetOrdinal("FileName")).Substring(0, 4) + @"\" + reader.GetString(reader.GetOrdinal("FileName")) + ".lrprev";
                            orientation = reader.GetString(reader.GetOrdinal("orientation"));
                        }
                    }
                }
                return GetPreview(path, aproximatePixelSize, orientation);
            }
            catch (Exception e)
            {
                return null;
            }
        }

        public BitmapImage GetPreview(string cacheFilePath, int approximatePixelSize /* Squared and compared to h*w */, string orientation)
        {
            // This is set up as public but is essentially an internal routine
            // But if you happen to know a cache file name, this will access it without using the preview database
            try
            {
                byte[] bytes = File.ReadAllBytes(cacheFilePath);

                // Find all JPG's inside of image by looking for boundaries of 0xFFD8 and 0xFFD9   
                int[,] startLen = new int[10, 4];  // 10 = more than we need, 0-4 as below
                const int Startpos = 0;
                const int Lenpos = 1;
                const int Heightpos = 2;
                const int Widthpos = 3; 

                int holdstart = -1;
                int holdheight = -1;
                int holdwidth = -1;
                int lastpositionsize = -1;
                int lastpositionstart = -1; 

                // First we will encounter the height/widths, then later (in binary) the start/len
                for (int i = 0; i < bytes.Length - 1; i++)
                {
                    // Note we are not bounds checking here because if any of these fail, the preview file is invalid, and we just fall into the catch 
                    if (holdstart == -1)
                    {   // We are still in the prologue where adobe puts the list
                        if (System.Text.Encoding.UTF8.GetString(bytes, i, 6) == "height")
                        {
                            int.TryParse(System.Text.Encoding.UTF8.GetString(bytes, i + 9, LengthToComma(ref bytes, i + 9, 20)), out holdheight);  // just let parse sort out the comma
                        }
                        if (System.Text.Encoding.UTF8.GetString(bytes, i, 5) == "width")
                        {
                            int.TryParse(System.Text.Encoding.UTF8.GetString(bytes, i + 8, LengthToComma(ref bytes, i + 8, 20)), out holdwidth);  // just let parse sort out the comma
                        }
                        if (holdheight > 0 && holdwidth > 0)
                        {
                            startLen[++lastpositionsize, Heightpos] = holdheight;
                            startLen[lastpositionsize, 3] = holdwidth; 
                            holdheight = holdwidth = -1; 
                        }
                    } 

                    if (bytes[i] == 0xFF && bytes[i + 1] == 0xD8)
                    {   // start
                        holdstart = i; 
                    }
                    else if (bytes[i] == 0xff && bytes[i + 1] == 0xD9)
                    {   // end
                        if (holdstart >= 0)
                        {
                            startLen[++lastpositionstart, Startpos] = holdstart;
                            startLen[lastpositionstart, Lenpos] = i - holdstart + 2; 
                        }
                    } 
                }

                // Bubble sort because I'm lazy, sorting by w*h, ascending
                // Use last item as swap area since we know it's already too big 
                for (int j = 0; j < lastpositionsize - 1; j++)
                {
                    for (int k = j + 1; k < lastpositionsize; k++)
                    {
                        if (startLen[j, Heightpos] * startLen[j, Widthpos] > startLen[k, Heightpos] * startLen[k, Widthpos])
                        {
                            for (int n = 0; n < 4; n++)
                            {
                                int t;
                                t = startLen[k, n];
                                startLen[k, n] = startLen[j, n];
                                startLen[j, n] = t;
                            }
                        }
                    }
                }
                int toReturn; // Return smallest if none are better
                for (toReturn = 0; toReturn <= lastpositionsize; toReturn++)
                {
                    if (Math.Sqrt(startLen[toReturn, Heightpos] * startLen[toReturn, Widthpos]) >= approximatePixelSize)
                    {
                        break;
                    }
                }
                toReturn = Math.Max(0, Math.Min(lastpositionsize, toReturn)); 
                BitmapImage retImage = new BitmapImage(); 
                retImage.BeginInit();
                retImage.StreamSource = new MemoryStream(bytes, startLen[toReturn, Startpos], startLen[toReturn, Lenpos]);
                retImage.Rotation = orientation == "AB" ? System.Windows.Media.Imaging.Rotation.Rotate0 : (orientation == "DA" ? System.Windows.Media.Imaging.Rotation.Rotate270 : (orientation == "BC" ? System.Windows.Media.Imaging.Rotation.Rotate90 : (orientation == "CD" ? System.Windows.Media.Imaging.Rotation.Rotate180 : System.Windows.Media.Imaging.Rotation.Rotate0))); 
                retImage.EndInit();
                while (retImage.IsDownloading)
                {
                    System.Threading.Thread.Sleep(10);
                }
                retImage.Freeze();
                return retImage;
            }
            catch (Exception e)
            {
                return null;
            }
        }

        private int LengthToComma(ref byte[] bytes, int start, int maxlook)
        {
            // Used to look ahead in the image data to find the next comma; part of parsing out the file sizes from the header
            try
            {
                int ret = 1;
                for (int i = start; i - start < maxlook; i++)
                {
                    if (bytes[i] == ',')
                    {
                        return i - start;
                    }
                }
                return ret;
            }
            catch
            {
                return 1;
            }
        }
    }
}
