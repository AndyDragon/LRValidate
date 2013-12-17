//  Copyright (c) LE Ferguson, LLC 2013 
// 
//  Limitations: 
//
//      - Does not deal with offline media (how does one know if offline vs. missing?)
//      - Does not deal with video (issue with on-disk image display - just blank)
//      - Does not deal with XMP files (ignores them) 
//      - DNG and raw display needs appropriate OS codec installed
//      - Virus scanners will impact speed
//
//  Errors: 
//      - Does not prevent someone from running it twice on the same catalog 
//      - Does not check for versions or incorrect catalog formats other than presence of our tables
//      - If from ShowImageDetails you ignore the last image (maybe only image) the review errors program does not close automatically
//      - Needs release notes somewhere/how and link on screen

namespace LRValidate
{
    using System;
    using System.Collections.Generic;
    using System.ComponentModel;
    using System.IO;
    using System.Security.Cryptography;
    using System.Text.RegularExpressions;
    using System.Threading;
    using System.Windows;
    using System.Windows.Documents;
    using System.Windows.Threading;

    public partial class MainWindow : Window
    {
        public static ReviewErrors ReviewErrorsNewErrors = null;               // Window object: We only run one of these at a time, and must know if it's running before this stops, so track if they are running here
        public static ReviewErrors ReviewErrorsRevalidateErrors = null;        // Window object: We only run one of these at a time, and must know if it's running before this stops, so track if they are running here

        public static Window ThisWindow;                                       // Keep a copy of this window so we can later refer to it from any thread easily.

        public static int FileFetchThreadsRunning = 0;                         // This is just an exposed counter of the number of threads running doing file image lookups in the display program

        public static System.Data.Common.DbConnection DbConn;                  // Connection is shared in threads within a window; different connection in different windows (which are on different threads) 
        public static string CatalogPath;                                      // Need this public so preview cache can use it

        public static int UpdatesThisCommit = 0;                               // How many database updates have been done since the last commit

        public static System.Data.SQLite.SQLiteTransaction MainTransaction;    // Transaction object used in larger data operations

        public static TimeCounts[] DateTimeArray = new TimeCounts[NumBuckets]; // Array of time/counts for revalidation to pass out to allow user to pick

        private const double SecondsBetweenUpdatesWhenBusy = 300;              // If we are really working hard only double check the updates every 5 minutes
        private const int FileWorkerThreads = 6;                               // How many worker threads will we use (separately for new/revalidate but same number) 
        private const int UpdatesPerCommit = 50;                               // How much data is updated in a batch to sqlite
        private const int NumBuckets = 10;                                     // How many possible buckets we offer for cutoff time selection 

        private static DateTime lastTimeUpdateRun = DateTime.MinValue;         // Time of last database status query (which we do infrequently) 
        private static int updateBusy = 0;                                     // Interlock variable to keep from having two updates running at once
        private static int commitBusy = 0;                                     // Interlock variable to keep from having two commits at the same time
        private static int databaseHasOurTables = -1;                          // Has database been initialized to contain our tables, -1=don't know yet, 0 = no, 1 = yes

        private static int newImagesCnt = -1;                                  // Approximate number of new images yet to be done (-1 = not known) 
        private static int revalidateImagesCnt = -1;                           // Approximate number of images to be revalidated based on cutoff date (-1 = not known) 
        private static int newErrorsCnt = -1;                                  // Approximate number of new images found to be in error (-1 = not known) 
        private static int revalidateErrorsCnt = -1;                           // Approximate number of images that were to be revalidated that were in error (-1 = not known) 

        private BackgroundWorker updateInfoWorkerThread = null;                // Used in (possibly) long running query of database for status/counts
        private BackgroundWorker newFilesWorkerThread = null;                  // Used as main driver loop for doing a find-new
        private BackgroundWorker revalidateFilesWorkerThread = null;           // Used as main driver loop for revalidating 
        private BackgroundWorker databaseHaveOurTablesThread = null;           // Used as worker for one-time check of a catalog (since it can hang "locked" for a while)

        private List<BackgroundWorker> initialThreadList = new List<BackgroundWorker>();        // Keep a list of individual file worker threads for find-new 
        private int initialThreadListBusy = 0;                                                  // Since lists are not thread safe, enforce synchronized updates through an interlocked variable in main thread only
        private List<BackgroundWorker> revalidateThreadList = new List<BackgroundWorker>();     // Keep a list of indivudal worker threads for revalidate 
        private int revalidateThreadListBusy = 0;                                               // Interlock variable to make thread safe 

        private bool cancelGivenForFindNew = false;                            // Has a cancel been requested of async run for find-new
        private bool cancelGivenForProcessExisting = false;                    // Has a cancel been requested of async run for processing revalidation 
        
        // This is the main routine for Lightroom Validate.  It has only one instance (per application invocation)
        // and in turn calls either (a) worker processes (no separate window), or (b) review errors windows (new or revalidate, which 
        // are run from the same window with two different meanings, or (c) help, or (d) revalidate date picker.  This window
        // remains the root window and each child is owned by it.
        // 
        // Error Handling: Each called routine or window must handle its own errors and not rethrow.
        public MainWindow()
        {
            // Initializes a new instance of the <see cref="MainWondow"> class
            // The main window will have only one instance (at least per catalog, though at present there is no lock-out to prevent someone from running it twice)
            InitializeComponent();
            CatalogFileSpec.Text = Properties.Settings.Default.DefaultCatalog;
            ThisWindow = this;
            UpdateMainWindowInfo();
        }

        public static void UpdateMainWindowInfo()
        {
            // Initiate query to update window; run in any thread
            // This runs through the dispatcher to hanlde cross-thread access
            //
            // It's important that the main thread not have blocking (long running) actions itself so that updates can proceed
            Action workMethod = () => ((MainWindow)ThisWindow).UpdateMainWindowInfo_Action();
            ((MainWindow)ThisWindow).DatabaseStatusLbl.Dispatcher.Invoke(DispatcherPriority.Normal, workMethod);
        }

        /* =============================================================================================================================================================
        //                 Event Handlers 
        //
        //      Note that most of these Count on the enabled/disabled state of controls being set correctly, and do not check fully 
        //      conditions on entry to ensure it is safe to run.  
        //
        // ============================================================================================================================================================*/

        private void BrowseBtn_Click(object sender, RoutedEventArgs e)
        {
            // Look up a file (catalog), always done in main thread as modal dialog
            Microsoft.Win32.OpenFileDialog dlg = new Microsoft.Win32.OpenFileDialog();
            dlg.DefaultExt = ".lrcat";
            dlg.Filter = "LR Catalog Files Files (*.lrcat)|*.lrcat";
            bool? result = dlg.ShowDialog();
            if (result == true)
            {
                Properties.Settings.Default.DefaultCatalog = CatalogFileSpec.Text = dlg.FileName; 
                Properties.Settings.Default.Save();
            }
        }

        private void OpenDatabase_Click(object sender, RoutedEventArgs e)
        {
            // Open a catalog for processing, All work done in main thread
            CatalogPath = CatalogFileSpec.Text;
            TryOpenDatabase(CatalogFileSpec.Text);
            UpdateMainWindowInfo();
            DoesDatabaseHaveOurTables();   // will set permanent variable but perhaps not instantly 
            UpdateMainWindowInfo();
        }

        private void Close_Catalog_Click(object sender, RoutedEventArgs e)
        {
            // Close a catalog from processing, All work done in main thread
            CloseAndDisposeDatabase();
        }

        private void InitDatabase_Click(object sender, RoutedEventArgs e)
        {
            // Initialize the database with our tables
            try
            {
                MessageBoxResult result = MessageBox.Show("Catalog will be initialized with a new table to support checksums. Existing data will not be modified in any way, but it is still wise to have a backup first.  Please confirm ready to initialize", "Confirm Initialization", MessageBoxButton.OKCancel);
                if (result == MessageBoxResult.OK)
                {
                    using (System.Data.Common.DbCommand cmd = DbConn.CreateCommand())
                    {
                        cmd.CommandText = @"Create Table LightroomValidateImages (ImageID int, InitialChecksumDateTime, LastValidateDateTime, checksum, Oldpath, Primary Key (ImageID));" +
                                           "Create Table LightroomValidateErrors (ImageID int, ErrorDetectedDateTime, InvalidChecksum, ValidationError, SuggestedAction, Primary Key (ImageID));";
                        cmd.ExecuteNonQuery();
                        MessageBox.Show("Database initialized - do a Find New next'.", "Initialization Complete", MessageBoxButton.OK, MessageBoxImage.Information);
                        databaseHasOurTables = 1;
                    }
                }
                UpdateMainWindowInfo();
            }
            catch (Exception e2)
            {
                MessageBox.Show("Unexpected error in InitDatabase_click, error=" + e2.Message, "Error", MessageBoxButton.OK, MessageBoxImage.Error);
                CloseAndDisposeDatabase();
            }
        }

        private void FindNewBtn_Click(object sender, RoutedEventArgs e)
        {
            // Start a find on new items, the find main loop runs in a separate thread (as do its workers)
            try
            {
                if (FindNewBtn.Content.ToString().StartsWith("Cancel"))
                {
                    cancelGivenForFindNew = true;
                    FindNewBtn.Content = NewButtonValue("Cancelling ()", newImagesCnt); // Actual values are entered later in update inside ()
                    return;
                }
                else if (FindNewBtn.Content.ToString().StartsWith("Cancelling"))
                {
                    return;   // Do nothing if we are already cancelling 
                }

                FindNewBtn.Content = NewButtonValue("Cancel ()", newImagesCnt);
                if (newFilesWorkerThread == null)
                {
                    List<string> argument = new List<string>();
                    argument.Add(RevalidateCutoffDateTime.Text);
                    newFilesWorkerThread = new BackgroundWorker();
                    newFilesWorkerThread.DoWork += new DoWorkEventHandler(DoAllInitialImages);
                    newFilesWorkerThread.RunWorkerCompleted += new RunWorkerCompletedEventHandler(DoAllInitialImages_Completed);
                    newFilesWorkerThread.RunWorkerAsync(argument);
                }
                UpdateMainWindowInfo();
            }
            catch (Exception e2)
            {
                MessageBox.Show("Unexpected error in FindNewBtn_Click, error=" + e2.Message, "Error", MessageBoxButton.OK, MessageBoxImage.Error);
            }
        }

        private void RevalidateBtn_Click(object sender, RoutedEventArgs e)
        {
            // Starts a revalidation run, which runs in a separate thread (as do its workers)
            try
            {
                if (RevalidateBtn.Content.ToString().StartsWith("Cancel"))
                {
                    cancelGivenForProcessExisting = true;
                    RevalidateBtn.Content = "Cancelling (" + revalidateImagesCnt.ToString() + ")";
                    return;
                }
                else if (RevalidateBtn.Content.ToString().StartsWith("Cancelling"))
                {
                    return; // Do nothing if we are already cancelling 
                }

                if (!ValidateCutoffDate())
                {
                    return;
                }
                RevalidateBtn.Content = NewButtonValue("Cancel ()", revalidateImagesCnt);
                if (revalidateFilesWorkerThread == null)
                {
                    List<object> arguments = new List<object>();
                    arguments.Add(RevalidateCutoffDateTime.Text);  // We can't touch this inside the routine but it's needed in the query 
                    revalidateFilesWorkerThread = new BackgroundWorker();
                    revalidateFilesWorkerThread.DoWork += new DoWorkEventHandler(RevalidateAllImages);
                    revalidateFilesWorkerThread.RunWorkerCompleted += new RunWorkerCompletedEventHandler(RevalidateAllImages_Completed);
                    revalidateFilesWorkerThread.RunWorkerAsync(arguments);
                }
                UpdateMainWindowInfo();
            }
            catch (Exception e2)
            {
                MessageBox.Show("Unexpected error in RevalidateBtn_Click, error=" + e2.Message, "Error", MessageBoxButton.OK, MessageBoxImage.Error);
            }
        }

        private void CleanDatabase_Click(object sender, RoutedEventArgs e)
        {
            // Clear out the database tables that are related to this program
            try
            {
                MessageBoxResult result = MessageBox.Show("Catalog will be cleared of tables related to this program. All past validation and checksums will be removed, and you can no longer validate.  Please confirm ready to Clean.", "Confirm Clean", MessageBoxButton.OKCancel);
                if (result == MessageBoxResult.OK)
                {
                    using (System.Data.Common.DbCommand cmd = DbConn.CreateCommand())
                    {
                        cmd.CommandText = @"Drop Table LightroomValidateImages; Drop Table LightroomValidateErrors";
                        cmd.ExecuteNonQuery();
                        databaseHasOurTables = 0;
                        MessageBox.Show("Database Cleaned.", "Clean Complete", MessageBoxButton.OK, MessageBoxImage.Information);
                    } // using cmd
                } // MessageBoxResult OK
                UpdateMainWindowInfo();
            }
            catch (Exception e2)
            {
                CloseAndDisposeDatabase();
                MessageBox.Show("Unexpected error in CleanDatabase_Click, error=" + e2.Message, "Error", MessageBoxButton.OK, MessageBoxImage.Error);
            }
        }

        private void RevalidateCutoffDateTime_LostFocus(object sender, RoutedEventArgs e)
        {
            // We need to validate the dates entered (there must be a better way
            ValidateCutoffDate();  // This routine does NOT require a valid date, it just warns if you don't have one
        }

        private void NewImagesInErrorBtn_Click(object sender, RoutedEventArgs e)
        {
            // Kick off a separate window (not thread) that will do a new image error review
            ReviewErrorsNewErrors = new ReviewErrors(ReviewErrors.ReviewType.NewErrors);
            ReviewErrorsNewErrors.Owner = this;
            ReviewErrorsNewErrors.Title = "Review New Image Validation Errors";
            ReviewErrorsNewErrors.Show();
            UpdateMainWindowInfo();
        }

        private void ExistingImagesInErrorBtn_Click(object sender, RoutedEventArgs e)
        {
            // Kick off a separate window (not thread) that will do a review of revalidated image errors
            ReviewErrorsRevalidateErrors = new ReviewErrors(ReviewErrors.ReviewType.RevalidateErrors);
            ReviewErrorsRevalidateErrors.Owner = this;
            ReviewErrorsRevalidateErrors.Title = "Review Re-Validation Errors";
            ReviewErrorsRevalidateErrors.Show();
            UpdateMainWindowInfo();
        }

        private void Hyperlink_RequestNavigate(object sender, System.Windows.Navigation.RequestNavigateEventArgs e)
        {
            // Navigate to donating website
            try
            {
                System.Diagnostics.Process.Start(e.Uri.ToString());
            }
            catch
            {   // Ignore navigation errors
                return;
            }
        }

        private void PickCutoffBtn_Click(object sender, RoutedEventArgs e)
        {
            // Summarize the current database values, and divide them up and present some options for the user
            int totalCounts;
            try
            {
                using (System.Data.Common.DbCommand cmd = DbConn.CreateCommand())
                {
                    // We need the total so we can parse out the rows as they come in
                    cmd.CommandText = @"select Count(*) as Count " +
                                        "from LightroomValidateImages lvi   " +
                                        "left join Adobe_images ai  on lvi.ImageID=ai.id_local  " +
                                        "where not exists  " +
                                        "           (select *  " +
                                        "            from LightroomValidateErrors lve  " +
                                        "            where lve.ImageID=lvi.ImageID)  " +
                                        "group by  Substr(LastValidateDateTime,1,16) ";
                    using (System.Data.Common.DbDataReader imageReader = cmd.ExecuteReader())
                    {
                        if (imageReader.Read())
                        {
                            totalCounts = imageReader.GetInt32(0);
                        }
                        else
                        {
                            MessageBox.Show("There are no items of any sort to revalidate, so you can't set the date/time with this routine", "No Items to Revalidate", MessageBoxButton.OK, MessageBoxImage.Warning);
                            return; // If there are no items then this call is irrelevant (message here?)
                        }
                    }
                    cmd.CommandText = @"select LastValidateDateTime as Datetime, Count(*) as Count " +
                                        "from LightroomValidateImages lvi   " +
                                        "left join Adobe_images ai  on lvi.ImageID=ai.id_local  " +
                                        "where not exists  " +
                                        "           (select *  " +
                                        "            from LightroomValidateErrors lve  " +
                                        "            where lve.ImageID=lvi.ImageID)  " +
                                        "group by  Substr(LastValidateDateTime,1,16) " +
                                        "order by 1 desc";
                    int lastBucket = -1;
                    using (System.Data.Common.DbDataReader imageReader = cmd.ExecuteReader())
                    {
                        while (imageReader.Read())
                        {
                            if (lastBucket == -1 || (DateTimeArray[lastBucket].Count > (totalCounts / NumBuckets) && (lastBucket < NumBuckets - 1)))
                            {
                                DateTimeArray[++lastBucket].Datetime = imageReader.GetDateTime(imageReader.GetOrdinal("Datetime"));
                                DateTimeArray[lastBucket].Count = 0; // We'll add below
                            }
                            DateTimeArray[lastBucket].Count += imageReader.GetInt32(imageReader.GetOrdinal("Count"));
                        }
                    }
                    for (int i = lastBucket - 1; i >= 0; i--)
                    {
                        DateTimeArray[i].Count += DateTimeArray[i + 1].Count;
                    }
                    PickRevalidateTime dlg = new PickRevalidateTime();
                    dlg.Owner = this;
                    dlg.ShowDialog();
                    UpdateMainWindowInfo();
                }
            }
            catch (Exception e2)
            {
                MessageBox.Show("Unexpected exception in PickCutoffBtn_Click, exception=" + e2.Message, "Unexpected Error", MessageBoxButton.OK, MessageBoxImage.Stop);
            }
        }

        private void ReleaseNotesLabel_PreviewMouseLeftButtonUp(object sender, System.Windows.Input.MouseButtonEventArgs e)
        {
            Release_Notes_and_Intro dlg = new Release_Notes_and_Intro();
            dlg.Owner = this;
            dlg.ShowDialog(); 
        }

        /* =============================================================================================================================================================
                          Utility Routines and Functions 
           ============================================================================================================================================================*/

        private void UpdateMainWindowInfo_Action()  // Action that the dispatcher invokes in the main thread only, but this hands off long running query to background
        {
            // Action routine that is called from dispather.  This is not necessarily on a separate thread from the UpdateMainWindowInfo() call 
            // and all this does is kick off (sometimes) a database query to pull counts, and RefreshControls().  Note that the 
            // async call will also do a RefreshControls() in its completion.
            if (databaseHasOurTables == 1 && DbConn != null && DbConn.State == System.Data.ConnectionState.Open)
            {
                // This is to run a query that might take a lot of time on a large database
                // If the query is already running we just let the one running complete and don't start another (and don't update the screen)
                if (Interlocked.Exchange(ref updateBusy, 1) == 0)
                {
                    if (updateInfoWorkerThread == null)
                    {   // Only run if not already running
                        // Since this is a pretty expensive query, if either a find or revalidate is running, we only run this at most every 20 seconds
                        // The counters are going to be updated automatically anyway, so the query is itself only a double check
                        if ((newFilesWorkerThread == null && revalidateFilesWorkerThread == null) || DateTime.Now.Subtract(lastTimeUpdateRun) > System.TimeSpan.FromSeconds(SecondsBetweenUpdatesWhenBusy))
                        {
                            lastTimeUpdateRun = DateTime.Now;
                            List<string> argument = new List<string>();
                            argument.Add(RevalidateCutoffDateTime.Text);
                            updateInfoWorkerThread = new BackgroundWorker();
                            updateInfoWorkerThread.DoWork += new DoWorkEventHandler(UpdateMainWindowInfo_DoWork);
                            updateInfoWorkerThread.RunWorkerCompleted += new RunWorkerCompletedEventHandler(UpdateMainWindowInfo_RunWorkerCompleted);
                            updateInfoWorkerThread.RunWorkerAsync(argument);
                        }
                    }
                    Interlocked.Exchange(ref updateBusy, 0);
                }
            }
            RefreshControls();
        }

        private void UpdateMainWindowInfo_DoWork(object sender, DoWorkEventArgs e)
        {
            // This is a very long running query if you have a big catalog, so it should be called infrequently and on its own thread
            List<string> argument = e.Argument as List<string>;
            string revalidateCutoffDateTime_copy = argument[0];
            using (System.Data.Common.DbCommand cmd = DbConn.CreateCommand())
            {
                cmd.CommandText = @"select " +
                                    "(select Count(*) from Adobe_images ai where ai.MasterImage is null and not exists (select * from LightroomValidateImages lvi where lvi.ImageID=id_local) and not exists (select * from LightroomValidateErrors lve where lve.ImageID=id_local)) as NewIMages,  " +
                                    "(select Count(*) from LightroomValidateImages lvi  left join Adobe_images ai  on lvi.ImageID=ai.id_local where not exists (select * from LightroomValidateErrors lve where lve.ImageID=lvi.ImageID) and LastValidateDateTime <= '" + revalidateCutoffDateTime_copy.Replace(' ', 'T') + "') as RevalidateImages,  " +
                                    "(select Count(*)  from LightroomValidateErrors lve where not exists (select * from LightroomValidateImages lvi where lvi.ImageID=lve.ImageID)) as NewErrors,  " +
                                    "(select Count(*) from LightroomValidateErrors lve where exists (select * from LightroomValidateImages lvi where lvi.ImageID=lve.ImageID)) as RevalidateErrors";
                using (System.Data.Common.DbDataReader countReader = cmd.ExecuteReader())
                {
                    countReader.Read();
                    List<int> arguments = new List<int>();
                    arguments.Add(countReader.GetInt32(countReader.GetOrdinal("NewImages")));
                    arguments.Add(countReader.GetInt32(countReader.GetOrdinal("RevalidateImages")));
                    arguments.Add(countReader.GetInt32(countReader.GetOrdinal("NewErrors")));
                    arguments.Add(countReader.GetInt32(countReader.GetOrdinal("RevalidateErrors")));
                    e.Result = arguments;
                    countReader.Close();
                } // using CountReader
            } // using cmd 
        }

        private string NewButtonValue(string oldcontent, int value)
        {
            // Several buttons have row counts in them, and replacing that (without impacting the rest of the string) is done in this routine
            // Expect the oldcontent to have a (stuff) in it, and replace what's in stuff based on the value (if -1 then use a dash for not-applicable) 
            return value < 0 ? Regex.Replace(oldcontent, @"\(.*\)", "(-)") : Regex.Replace(oldcontent, @"\(.*\)", "(" + value.ToString() + ")");
        }

        private void RefreshControls()
        {
            // This routine is responsible for not only refreshing the control state and contents, but in making decisions on which 
            // control should be enabled based on other state information in the main routine.
            if (DbConn == null || DbConn.State == System.Data.ConnectionState.Closed)
            {
                DatabaseStatusLbl.Text = "Catalog is not yet open";
                DatabaseStatusLbl.Foreground = new System.Windows.Media.SolidColorBrush(System.Windows.Media.Colors.DarkGreen);
            } 
            else if (DbConn.State != System.Data.ConnectionState.Open)
            {   // Not sure what these might be but show them
                DatabaseStatusLbl.Text = "Catalog is " + DbConn.State.ToString();
                DatabaseStatusLbl.Foreground = new System.Windows.Media.SolidColorBrush(System.Windows.Media.Colors.Red);
            }
            else
            {
                if (databaseHasOurTables < 0)
                {
                    DatabaseStatusLbl.Text = "Catalog is open, checking status";
                    DatabaseStatusLbl.Foreground = new System.Windows.Media.SolidColorBrush(System.Windows.Media.Colors.Red);
                }
                else if (databaseHasOurTables == 0)
                {
                    DatabaseStatusLbl.Text = "Catalog is open but not initialized for checking";
                    DatabaseStatusLbl.Foreground = new System.Windows.Media.SolidColorBrush(System.Windows.Media.Colors.Yellow);
                }
                else
                {
                    DatabaseStatusLbl.Text = "Catalog is open, and has been initialized";
                    DatabaseStatusLbl.Foreground = new System.Windows.Media.SolidColorBrush(System.Windows.Media.Colors.Green);
                }
            }

            int threads = (((newFilesWorkerThread == null) ? 0 : 1) + initialThreadList.Count)
                        + (((revalidateFilesWorkerThread == null) ? 0 : 1) + revalidateThreadList.Count)
                        + ((updateInfoWorkerThread == null) ? 0 : 1)
                        + FileFetchThreadsRunning; 
                          
            ThreadString.Text = threads == 0 ? string.Empty : "Busy: Threads in use " + threads.ToString();

            CatalogFileSpec.IsEnabled = DbConn == null || (DbConn.State == System.Data.ConnectionState.Closed);
            BrowseBtn.IsEnabled = DbConn == null || (DbConn.State == System.Data.ConnectionState.Closed);
            Close_Catalog.IsEnabled = (threads == 0) && (ReviewErrorsRevalidateErrors == null) && (ReviewErrorsNewErrors == null) && (!(DbConn == null || (DbConn.State == System.Data.ConnectionState.Closed)));
            OpenDatabase.IsEnabled = DbConn == null || (DbConn.State == System.Data.ConnectionState.Closed);
            InitDatabase.IsEnabled = !(DbConn == null || (DbConn.State == System.Data.ConnectionState.Closed)) && (databaseHasOurTables == 0);
            FindNewBtn.IsEnabled = (databaseHasOurTables == 1) && newImagesCnt > 0;
            RevalidateBtn.IsEnabled = (databaseHasOurTables == 1) && revalidateImagesCnt > 0;
            CleanDatabase.IsEnabled = (databaseHasOurTables == 1) && Close_Catalog.IsEnabled;  // We can't clean if we can't lose (for whatever reason)
            NewImagesInErrorBtn.IsEnabled = (ReviewErrorsNewErrors == null) && (databaseHasOurTables == 1) && newErrorsCnt > 0;
            ExistingImagesInErrorBtn.IsEnabled = (ReviewErrorsRevalidateErrors == null) && (databaseHasOurTables == 1) && revalidateErrorsCnt > 0;
            PickCutoffBtn.IsEnabled = RevalidateCutoffDateTime.IsEnabled = (databaseHasOurTables == 1) && (revalidateFilesWorkerThread == null); // Match revalidate as that's when we parse the date the last time

            NewImagesInErrorBtn.Content = NewButtonValue(NewImagesInErrorBtn.Content.ToString(), newErrorsCnt);
            ExistingImagesInErrorBtn.Content = NewButtonValue(ExistingImagesInErrorBtn.Content.ToString(), revalidateErrorsCnt);
            FindNewBtn.Content = NewButtonValue(FindNewBtn.Content.ToString(), newImagesCnt);
            RevalidateBtn.Content = NewButtonValue(RevalidateBtn.Content.ToString(), revalidateImagesCnt);
        }

        private void UpdateMainWindowInfo_RunWorkerCompleted(object sender, RunWorkerCompletedEventArgs e)
        {
            // This receives values from the other thread's database query, and passes back the values from the database call and let's the refresh do the work (both must run in the main window thread) 
            List<int> returns = e.Result as List<int>;
            newImagesCnt = returns[0];
            revalidateImagesCnt = returns[1];
            newErrorsCnt = returns[2];
            revalidateErrorsCnt = returns[3];
            updateInfoWorkerThread = null;
            RefreshControls();
        }

        private string GetMD5HashFromFile(string fileName)
        {
            // Reads a file and calculates checksum.
            // If it has a problem it catches it, but then let caller handle errors in the return string
            // return them encoded as the word "Error:" at the beginning of the string
            try
            {
                using (var md5 = MD5.Create())
                using (var stream = new BufferedStream(File.OpenRead(fileName), 12000000))
                {
                    return BitConverter.ToString(md5.ComputeHash(stream)).Replace("-", string.Empty);
                }
            }
            catch (Exception e)
            {
                return ("Error: " + e.Message).Replace("'", string.Empty);
            }
        }

        private void CheckAddToTransaction()
        {
            // Check current transaction Count, and if zero start one, if overdue, commit and start one, otherwise just increment.
            while (Interlocked.Exchange(ref commitBusy, 1) == 1)
            {
                System.Threading.Thread.Sleep(100);
            }
            if (UpdatesThisCommit >= UpdatesPerCommit)
            {
                MainTransaction.Commit();  // Note this looks just like the code for CommitTransaction(), but we can't call that as it will deadlock waiting for the critical section semaphore.
                MainTransaction.Dispose();
                UpdatesThisCommit = 0;
            }
            if (UpdatesThisCommit == 0)
            {
                MainTransaction = (System.Data.SQLite.SQLiteTransaction)DbConn.BeginTransaction();
            }
            UpdatesThisCommit++;
            Interlocked.Exchange(ref commitBusy, 0);
        }

        private void CommitTransaction()
        {
            while (Interlocked.Exchange(ref commitBusy, 1) == 1)
            {
                System.Threading.Thread.Sleep(100);
            }
            if (MainTransaction != null)
            {
                if (UpdatesThisCommit > 0)
                {   // For some reason I couldn't check the disposed flag in the transaction so am using the counter to keep track if it is open
                    MainTransaction.Commit();
                    MainTransaction.Dispose();
                    UpdatesThisCommit = 0;
                }
            }
            Interlocked.Exchange(ref commitBusy, 0);
        }

        private void DoInitialOneImage(object sender, DoWorkEventArgs e)
        {
            // Used in worker thread to do one specific image and update
            List<object> gl = e.Argument as List<object>; // Pull out the arguments passed in
            int imageID = (int)gl[0];
            string imagePath = (string)gl[1];

            CheckAddToTransaction();
            using (System.Data.Common.DbCommand cmd = DbConn.CreateCommand())
            {
                if (File.Exists(imagePath))
                {
                    string checksum = GetMD5HashFromFile(imagePath);
                    {
                        if (checksum.Substring(0, 5) != "Error")
                        {
                            cmd.CommandText = "Insert into LightroomValidateImages (ImageID, InitialChecksumDateTime, LastValidateDateTime, checksum, oldPath) values ("
                                              + imageID.ToString() + ", strftime('%Y-%m-%dT%H:%M:%S','now','localtime'), strftime('%Y-%m-%dT%H:%M:%S','now','localtime'), '" + checksum + "', '" + imagePath + "')";
                        }
                        else
                        {   // Image is not valid and couldn't be read for checksum
                            cmd.CommandText = "Insert into LightroomValidateErrors (ImageID, ErrorDetectedDateTime, ValidationError, SuggestedAction) values ("
                                                  + imageID.ToString() + ", strftime('%Y-%m-%dT%H:%M:%S','now','localtime'), 'Initial validation of image found file, but could not read for checksum','" + checksum + "; possible disk error or bad file structure. Find issue, and mark this error to ignore and thus revalidate.')";
                        }
                    } // using 
                }
                else
                {   // File doesn't exist 
                    cmd.CommandText = "Insert into LightroomValidateErrors (ImageID, ErrorDetectedDateTime, ValidationError, SuggestedAction) values ("
                                          + imageID.ToString() + ", strftime('%Y-%m-%dT%H:%M:%S','now','localtime'), 'Initial validation of image did not find file where lightroom said it was.','Lightroom said file was at (" + imagePath + "); File may be offline (NOT SUPPORTED), or inappropriately moved.  Consider using Lightroom''s Find Missing; when dealt with mark to ignore and re-find.')";
                    newErrorsCnt++;
                }
                cmd.ExecuteNonQuery();
            }
            newImagesCnt--;  // Update our running (approximate) Count
            UpdateMainWindowInfo();
        }

        private void DoAllInitialImages(object sender, DoWorkEventArgs e)
        {
            // Main loop (in its own thread) that drives finding new images
            List<string> argument = e.Argument as List<string>; // Pull out the arguments passed in
            string revalidateCutoffDateTimeString = argument[0];
            try
            {
                using (System.Data.Common.DbCommand cmd = DbConn.CreateCommand())
                {
                    cmd.CommandText = @"select ai.id_local as ImageID, rf.absolutePath || lfo.pathFromRoot || lf.baseName || '.' ||  lf.extension as imagePath " +
                                        "from Adobe_images ai  " +
                                        "inner join AgLibraryFile lf on lf.id_local = ai.rootFile " +
                                        "inner join AgLibraryFolder lfo on lfo.id_local = lf.folder " +
                                        "inner join AgLibraryRootFolder rf on rf.id_local = rootFolder " +
                                        "where ai.MasterImage is null " +
                                        "  and not exists  " +
                                        "( " +
                                        "    select *  " +
                                        "    from LightroomValidateImages lvi  " +
                                        "    where lvi.ImageID=ai.id_local " +
                                        ") " +
                                        "  and not exists " +
                                        "( " +
                                        "    select *  " +
                                        "    from LightroomValidateErrors lve  " +
                                        "    where lve.ImageID=ai.id_local " +
                                        ") "; 
                    using (System.Data.Common.DbDataReader imageReader = cmd.ExecuteReader())
                    {
                        while (imageReader.Read() && !cancelGivenForFindNew)
                        {
                            List<object> arguments = new List<object>();
                            arguments.Add(imageReader.GetInt32(imageReader.GetOrdinal("ImageID")));
                            arguments.Add(imageReader.GetString(imageReader.GetOrdinal("imagePath")));
                            while (initialThreadList.Count >= FileWorkerThreads)
                            {
                                Thread.Sleep(1);
                            }
                            BackgroundWorker t = new BackgroundWorker();
                            t.DoWork += new DoWorkEventHandler(DoInitialOneImage);
                            t.RunWorkerCompleted += new RunWorkerCompletedEventHandler(DoInitialWorkerDone);
                            while (Interlocked.Exchange(ref initialThreadListBusy, 1) == 1)
                            {
                                Thread.Sleep(1);
                            }
                            initialThreadList.Add(t);
                            initialThreadList[initialThreadList.Count - 1].RunWorkerAsync(arguments);
                            Interlocked.Exchange(ref initialThreadListBusy, 0);
                        }
                        imageReader.Close();
                        while (initialThreadList.Count > 0)
                        {
                            Thread.Sleep(100);
                        }
                        CommitTransaction();
                        cancelGivenForFindNew = false;
                        Action<string> workMethod = (str) => FindNewBtn.Content = str;
                        FindNewBtn.Dispatcher.Invoke(DispatcherPriority.Normal, workMethod, "Find New ()");   // The next call will do the update inside the ()
                        UpdateMainWindowInfo();
                    }
                }
            }
            catch (Exception e2)
            {
                MessageBox.Show("Unexpected error in DoAllInitialImages, error=" + e2.Message, "Error", MessageBoxButton.OK, MessageBoxImage.Error);
            }
        }

        private void DoInitialWorkerDone(object sender, RunWorkerCompletedEventArgs e)
        {
            // Worker completion thread, runs in context of the main loop thread
            while (Interlocked.Exchange(ref initialThreadListBusy, 1) == 1)
            {
                Thread.Sleep(1);
            }
            initialThreadList.Remove(sender as BackgroundWorker);
            Interlocked.Exchange(ref initialThreadListBusy, 0);
        }

        private void DoAllInitialImages_Completed(object sender, RunWorkerCompletedEventArgs e)
        {
            // Main find-new loop completion thread, runs in context of main window and marks itself done
            newFilesWorkerThread = null;
            UpdateMainWindowInfo();
        }

        private void DoRevalidateOneImage(object sender, DoWorkEventArgs e)
        {
            // Worker thread routine for revalidating a single image file
            List<object> gl = e.Argument as List<object>;
            int imageID = (int)gl[0];
            string currentImagePath = (string)gl[1];
            string previousChecksum = (string)gl[2];
            string oldPath = (string)gl[3];
            string lastChecksumDateTime = (string)gl[4];
            string lastValidationDateTime = (string)gl[5];
            string revalidateCutoffDateTime_string = (string)gl[6];

            CheckAddToTransaction();
            using (System.Data.Common.DbCommand cmd = DbConn.CreateCommand())
            {
                CheckAddToTransaction();
                if (File.Exists(currentImagePath))
                {
                    string checksum = GetMD5HashFromFile(currentImagePath);
                    if (checksum == previousChecksum)
                    {
                        cmd.CommandText = @"Update LightroomValidateImages set LastValidateDateTime = strftime('%Y-%m-%dT%H:%M:%S','now','localtime') where ImageID=" + imageID.ToString();
                    }
                    else if (checksum.Substring(0, 5) == "Error")
                    {   // checksum itself failed
                        cmd.CommandText = "Insert into LightroomValidateErrors (ImageID, ErrorDetectedDateTime, ValidationError, SuggestedAction) values ("
                                              + imageID.ToString() + ", strftime('%Y-%m-%dT%H:%M:%S','now','localtime'), 'Revalidation found file but read for checksum failed.','Possible disk error or bad file structure? " + checksum + ". Ensure file is readable.  Try viewing here or in Lightroom.  Mark this error to ignore to try again after correcting problem.')";
                        revalidateErrorsCnt++;
                    }
                    else
                    {   // Image is valid but had a mis-matched checksum
                        cmd.CommandText = "Insert into LightroomValidateErrors (ImageID, ErrorDetectedDateTime, InvalidChecksum, ValidationError, SuggestedAction) values ("
                                            + imageID.ToString() + ", strftime('%Y-%m-%dT%H:%M:%S','now','localtime'), '" + checksum + "', 'Revalidation of found checksum mismatch.', 'File may have been changed for metadata writes or direct edits, but ensure the changes were intended.  Mark to Accept if changes are OK; mark to Ingore and revalidate later if you want to recover/check/change and try again.')";
                        revalidateErrorsCnt++;
                    }
                }
                else
                {   // File no longer exists 
                    cmd.CommandText = "Insert into LightroomValidateErrors (ImageID, ErrorDetectedDateTime,  ValidationError, SuggestedAction) values ("
                                        + imageID.ToString() + ", strftime('%Y-%m-%dT%H:%M:%S','now','localtime'),  'Revalidation of file did not find file at location Lightroom provided (" + currentImagePath + ").', 'Confirm file wasn''t inappropriately moved.  Last seen at (" + oldPath + "). If moved offline - NOT SUPPORTED YET.  Consider using Lightroom Find Missing.  Mark this item to ignore to revalidate (you cannot accept it).')";
                    revalidateErrorsCnt++;
                }
                cmd.ExecuteNonQuery();
            } // using 
            revalidateImagesCnt--;  // Update our running (approximate) Count
            UpdateMainWindowInfo();
        }

        private void RevalidateAllImages(object sender, DoWorkEventArgs e)
        {
            // Main loop to revalidate all images, this is running in its own thread, as is each worker it spans
            List<object> gl = e.Argument as List<object>; // Pull out the arguments passed in
            string revalidateCutoffDateTimeString = (string)gl[0];
            try
            {
                using (System.Data.Common.DbCommand cmd = DbConn.CreateCommand())
                {
                    cmd.CommandText = @"select ai.id_local as ImageID, rf.absolutePath || lfo.pathFromRoot || lf.baseName || '.' || lf.extension as currentImagePath, lvi.checksum, lvi.Oldpath, lvi.InitialChecksumDateTime, lvi.LastValidateDateTime " +
                                        "from LightroomValidateImages lvi  " +
                                        "left join Adobe_images ai  on lvi.ImageID=ai.id_local " +
                                        "left join AgLibraryFile lf on lf.id_local = ai.rootFile " +
                                        "left join AgLibraryFolder lfo on lfo.id_local = lf.folder " +
                                        "left join AgLibraryRootFolder rf on rf.id_local = rootFolder " +
                                        "where LastValidateDateTime < '" + revalidateCutoffDateTimeString.Replace(' ', 'T') + "' " +
                                        "  and not exists (select * from LightroomValidateErrors lve where lve.ImageID=lvi.ImageID)";
                    using (System.Data.Common.DbDataReader imageReader = cmd.ExecuteReader())
                    {
                        while (imageReader.Read() && !cancelGivenForProcessExisting)
                        {
                            List<object> arguments = new List<object>();
                            arguments.Add(imageReader.GetInt32(imageReader.GetOrdinal("ImageID")));
                            arguments.Add(imageReader.GetString(imageReader.GetOrdinal("currentImagePath")));
                            arguments.Add(imageReader.GetString(imageReader.GetOrdinal("checksum")));
                            arguments.Add(imageReader.GetString(imageReader.GetOrdinal("Oldpath")));
                            arguments.Add(imageReader.GetString(imageReader.GetOrdinal("InitialChecksumDateTime")));
                            arguments.Add(imageReader.GetString(imageReader.GetOrdinal("LastValidateDateTime")));
                            arguments.Add(revalidateCutoffDateTimeString);

                            while (revalidateThreadList.Count >= FileWorkerThreads)
                            {
                                Thread.Sleep(1);
                            }
                            BackgroundWorker t = new BackgroundWorker();
                            t.DoWork += new DoWorkEventHandler(DoRevalidateOneImage);
                            t.RunWorkerCompleted += new RunWorkerCompletedEventHandler(DoRevalidateWorkerDone);
                            while (Interlocked.Exchange(ref revalidateThreadListBusy, 1) == 1)
                            {
                                Thread.Sleep(1);
                            }
                            revalidateThreadList.Add(t);
                            revalidateThreadList[revalidateThreadList.Count - 1].RunWorkerAsync(arguments);
                            Interlocked.Exchange(ref revalidateThreadListBusy, 0);
                        } // while
                        imageReader.Close();
                        while (revalidateThreadList.Count > 0)
                        {
                            Thread.Sleep(100);
                        }
                        cancelGivenForProcessExisting = false;
                        CommitTransaction();
                        Action<string> workMethod = (str) => RevalidateBtn.Content = str;
                        RevalidateBtn.Dispatcher.Invoke(DispatcherPriority.Normal, workMethod, "Revalidate ()"); // Actual value in () will be filled in during update that follows
                        UpdateMainWindowInfo();  // Do synchronous update here in case the last running update didn't
                    } // using imageReader
                } // cmd
            }
            catch (Exception e2)
            {
                MessageBox.Show("Unexpected error in RevalidateAllImages, error=" + e2.Message, "Error", MessageBoxButton.OK, MessageBoxImage.Error);
            }
        }

        private void DoRevalidateWorkerDone(object sender, RunWorkerCompletedEventArgs e)
        {
            // Completion routine for individual per-file workers for revalidation 
            while (Interlocked.Exchange(ref revalidateThreadListBusy, 1) == 1)
            {
                Thread.Sleep(1);
            }
            revalidateThreadList.Remove(sender as BackgroundWorker);
            Interlocked.Exchange(ref revalidateThreadListBusy, 0);
        }

        private void RevalidateAllImages_Completed(object sender, RunWorkerCompletedEventArgs e)
        {
            // Completion routine for main revalidate loop, basically just marks itself done 
            revalidateFilesWorkerThread = null;
            UpdateMainWindowInfo();
        }

        private bool ValidateCutoffDate() // Check if date entered is a valid date and time
        {
            // Is entered date valid 
            DateTime d;
            bool isTimeOK = DateTime.TryParse(RevalidateCutoffDateTime.Text, out d);
            if (!isTimeOK)
            {
                MessageBox.Show("Enter a valid date/time (format MM/DD/YY HH:MM, using 24 hour time), and the validation will only affect images not validated since that time.", "Bad Validation Date/Time", MessageBoxButton.OK, MessageBoxImage.Error);
                return false;
            }
            else
            {
                RevalidateCutoffDateTime.Text = RevalidateCutoffDateTime.Text = DateTime.Parse(RevalidateCutoffDateTime.Text).ToString("yyyy-MM-dd HH:mm:ss");
                UpdateMainWindowInfo();  // Refresh (while we wait) the screen info
                return true;
            }
        }

        private void DoesDatabaseHaveOurTables()
        {
            // Check if the database has been initialized (the work is done in a separate thread) 
            // and updates (later) the databaseHasOurTables field
            if (databaseHaveOurTablesThread != null)
            {
                return;  // already running 
            }
            databaseHaveOurTablesThread = new BackgroundWorker();
            databaseHaveOurTablesThread.DoWork += new DoWorkEventHandler(DoesDatabaseHaveOurTables_DoWork);
            databaseHaveOurTablesThread.RunWorkerCompleted += new RunWorkerCompletedEventHandler(DoesDatabaseHaveOurTables_Completed);
            databaseHaveOurTablesThread.RunWorkerAsync();
        }

        private void DoesDatabaseHaveOurTables_DoWork(object sender, DoWorkEventArgs e)
        {
            // Worker routine to check if database has our tables -- note this is the main place that gets a locked record if Lightroom was already running 
            try
            {
                if (DbConn != null && DbConn.State == System.Data.ConnectionState.Open)
                {
                    using (System.Data.Common.DbCommand cmd = DbConn.CreateCommand())
                    {
                        cmd.CommandText = @"SELECT name FROM sqlite_master WHERE type='table' AND name='LightroomValidateImages';";
                        cmd.CommandTimeout = 3; // Don't make the user wait long 
                        using (System.Data.Common.DbDataReader reader = cmd.ExecuteReader())
                        {
                            databaseHasOurTables = reader.HasRows ? 1 : 0;
                        }
                    }
                }
                else
                {
                    newImagesCnt = revalidateImagesCnt = newErrorsCnt = revalidateErrorsCnt = -1;
                    databaseHasOurTables = -1;
                }
            }
            catch (Exception e3)
            {
                databaseHasOurTables = -1;
                MessageBox.Show("Catalog could not be read to check status; Do you perhaps have Lightroom open, if so close and re-try (status returned was " + e3.Message + ")", "Catalog cannot open", MessageBoxButton.OK, MessageBoxImage.Exclamation);
                CloseAndDisposeDatabase();  // since we can't really open it correctly let's mark it closed
            }
        }

        private void DoesDatabaseHaveOurTables_Completed(object sender, RunWorkerCompletedEventArgs e)
        {
            // Completion routine from checking for our tables, mainly to mark itself done
            databaseHaveOurTablesThread = null;
            UpdateMainWindowInfo();
        }

        private void TryOpenDatabase(string dbPath)  // If successful, leaves DbConn with an open status 
        {
            // Open the catalog specified (if possible) 
            try
            {
                if (File.Exists(dbPath))
                {
                    try
                    {
                        string connString = string.Format(@"Data Source={0}; Pooling=false; FailIfMissing=true;", dbPath);
                        using (var factory = new System.Data.SQLite.SQLiteFactory())
                        {
                            DbConn = factory.CreateConnection();
                            DbConn.ConnectionString = connString;
                            DbConn.Open();
                            RevalidateCutoffDateTime.Text = DateTime.Now.ToLocalTime().ToString("yyyy-MM-dd HH:mm:ss");
                            UpdateMainWindowInfo();
                        } // using factory
                    }
                    catch (Exception e)
                    {
                        MessageBox.Show("Lightroom catalog '" + dbPath + "' found but could not be opened; error received = '" + e.Message + "'.  Correct before proceeding.", "Catalog ", MessageBoxButton.OK, MessageBoxImage.Warning);
                    }
                }
                else
                {
                    MessageBox.Show("Lightroom catalog '" + dbPath + "' was not found.  Correct before proceeding.", "Catalog Not Found", MessageBoxButton.OK, MessageBoxImage.Warning);
                }
            }
            catch (Exception e)
            {
                MessageBox.Show("Lightroom catalog '" + dbPath + "' could not be accessed; error received = '" + e.Message + "'.  Correct before proceeding.", "Catalog Not Accessible", MessageBoxButton.OK, MessageBoxImage.Warning);
            }
        }

        private void CloseAndDisposeDatabase()  // Cleanly close the database if open, and regardless of state so no errors can be returned
        {
            // Close the current database, unconditionally, trapping and ignoring most errors 
            try
            {
                if (DbConn != null && DbConn.State != System.Data.ConnectionState.Closed)
                {
                    DbConn.Close();
                }
            }
            catch (Exception e)
            {
            }
            try 
            { 
                DbConn.Dispose(); 
            }
            catch (Exception e)
            { 
            }
            DbConn = null;
            databaseHasOurTables = -1;
            newImagesCnt = revalidateImagesCnt = newErrorsCnt = revalidateErrorsCnt = -1;
            UpdateMainWindowInfo();
        }

        public struct TimeCounts
        {   // Simple structure we can hold in an array to calculate cutoff times for the revalidate cutoff.
            public DateTime Datetime;
            public int Count;

            public TimeCounts(DateTime p1, int p2)
            {
                Datetime = p1;
                Count = p2;
            }
        }
    }
}
