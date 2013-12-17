//  Copyright (c) LE Ferguson, LLC 2013 
namespace LRValidate
{
    using System;
    using System.Collections.Generic;
    using System.Linq;
    using System.Windows;
    using System.Windows.Documents;

    public partial class ReviewErrors : Window
    {
        // This reviews either regular or new errors (but only one at a time per window -- though it may be open for each way on two different windows) 

        // We use the DbConn from the main window in this one, so that all operations occur on the same connections, to minimize chances of deadlock 
        // We aren't doing anything that should conflict (though it is possible), but we are leaving transactions open for performance reasons
        // This way each commit will commit all, and not block pending some other commit.
        public System.Data.DataTable Dt2;
        public ReviewType ThisRunType;

        // These items, however, are local to each instance of the window; they need to be global as they are shared between routines inside this window
        // No static items should be used here
        private System.Data.SQLite.SQLiteDataAdapter dataAdp;
        private System.Data.Common.DbCommand cmd;

        public ReviewErrors(ReviewType typeOfReview)
        {
            InitializeComponent();
            ThisRunType = typeOfReview;
        }

        public enum ReviewType 
        {   // Simple enum to tell us what kind of error review we are doing
            NewErrors, RevalidateErrors 
        }

        public bool MarkItemsToIgnore(List<System.Data.DataRowView> l) // Called from form by click uses selected items, deletes from not only the database but the data table
        {
            try
            {
                using (System.Data.SQLite.SQLiteTransaction xact = (System.Data.SQLite.SQLiteTransaction)MainWindow.DbConn.BeginTransaction())
                using (cmd = MainWindow.DbConn.CreateCommand())
                {
                    foreach (System.Data.DataRowView rv in l)
                    {
                        cmd.CommandText = "Delete from LightroomValidateErrors where ImageID=" + rv.Row.ItemArray[0].ToString();
                        cmd.ExecuteNonQuery();
                        Dt2.Rows.Remove(rv.Row);
                    }
                    xact.Commit();
                }
                MainWindow.UpdateMainWindowInfo();   // Start an async update on the main window display to reflect what we just changed
                return true;
            }
            catch (Exception e2)
            {
                MessageBox.Show("Unexpected error in MarkItemsToIgnore (ReviewErrors); error received = '" + e2.Message + "'.  Correct before proceeding.", "Ignore Error ", MessageBoxButton.OK, MessageBoxImage.Error);
                return false;
            }
        }

        public bool MarkItemsToAccept(List<System.Data.DataRowView> l)
        {
            try
            {
                // Because it otherwise complains the collection has been modified and will not enumerate, we copy the list to a variable and iterate over it.
                // There's probably some way to cast this assignment instead of iterate to create the list but this works...
                using (System.Data.SQLite.SQLiteTransaction xact = (System.Data.SQLite.SQLiteTransaction)MainWindow.DbConn.BeginTransaction())
                using (cmd = MainWindow.DbConn.CreateCommand())
                {
                    foreach (System.Data.DataRowView rv in l)
                    {
                        cmd.CommandText = @"Update LightroomValidateImages set LastValidateDateTime = strftime('%Y-%m-%dT%H:%M:%S','now','localtime'),  " +
                                            "       checksum='" + rv.Row.ItemArray[2].ToString() + "', " +
                                            "       oldPath='" + rv.Row.ItemArray[5].ToString() + "' " +
                                            "where ImageID=" + rv.Row.ItemArray[0].ToString() + "; " +
                                            "Delete from LightroomValidateErrors where ImageID=" + rv.Row.ItemArray[0].ToString();
                        cmd.ExecuteNonQuery();
                        Dt2.Rows.Remove(rv.Row);
                    }
                    xact.Commit();
                    MainWindow.UpdateMainWindowInfo();   // Start an async update on the main window display to reflect what we just changed
                    return true;
                }
            }
            catch (Exception e2)
            {
                MessageBox.Show("Unexpected error in MarkItemsToAccept (ReviewErrors); error received = '" + e2.Message + "'.  Correct before proceeding.", "Accept Error ", MessageBoxButton.OK, MessageBoxImage.Error);
                return false;
            }
        }

        private void Window_Closing(object sender, System.ComponentModel.CancelEventArgs e)
        {
            try
            {   // Clean up anything we created  
                if (Dt2 != null)
                {
                    Dt2.Dispose();
                }
                if (dataAdp != null)
                {
                    dataAdp.Dispose();
                }
                if (cmd != null)
                {
                    cmd.Dispose();
                }
                if (ThisRunType == ReviewType.NewErrors)
                {
                    MainWindow.ReviewErrorsNewErrors = null;
                }
                else
                {
                    MainWindow.ReviewErrorsRevalidateErrors = null;
                }
                MainWindow.UpdateMainWindowInfo();   // Start an async update on the main window display to reflect what we just changed
            }
            catch (Exception e2)
            {
                MessageBox.Show("Unexpected error in Window_Closing (ReviewErrors); error received = '" + e2.Message + "'.  Correct before proceeding.", "Catalog ", MessageBoxButton.OK, MessageBoxImage.Error);
                throw;
            }
        }

        private void Window_Loaded(object sender, RoutedEventArgs e)
        {
            try
            {
                // Have to do these things here not initialization since the calling window hasn't set owner until constructor finishes (which runs initialize)
                using (cmd = MainWindow.DbConn.CreateCommand())
                {
                    // We only fill this once, to be less confusing (e.g. if the user is simultaneously still doing a find-new); they can exit and come back in if so
                    //
                    //  For this we fill the data table in the same format for both new and revalidate, with one clue  (update this list in ShowImageDetails also)
                    //
                    //      0 = ImageID
                    //      1 = Error Detected Time
                    //      2 = Invalid checksum - null for new images, but also null for revalidate if unreadable
                    //      3 = Validation Error Message (english for display)
                    //      4 = Suggested action message (english for display)
                    //      5 = Image Path per Lightroom (for existing images only, otherwise null) 
                    //      6 = image path from last time validated (if previously validated) 
                    //      7 = orientation per lightroom (force rotation of bitmap)
                    //      8 = Last Validation  Date/Time (null for new images) 
                    //      9 = Initial checksum Date/Time (null for new images) 
                    //      
                    //  Note that column labels ARE relevant for visibility (see below) but access is by ordinal position, so don't change either without care
                    cmd.CommandText = "Select lve.ImageID, lve.ErrorDetectedDateTime, lve.InvalidChecksum, lve.ValidationError, lve.SuggestedAction, ifnull(rf.absolutePath || lfo.pathFromRoot || lf.baseName || '.' ||  lf.extension,'') as LRImagePath, lvi.oldPath, ai.orientation, lvi.LastValidateDateTime, lvi.InitialChecksumDateTime " +
                                        "from LightroomValidateErrors lve " +
                                        (ThisRunType == ReviewType.NewErrors ? "left " : "inner ") + "join LightroomValidateImages lvi on lvi.ImageID=lve.ImageID " +
                                        "left join Adobe_images ai on ai.id_local=lve.ImageID " +
                                        "left join AgLibraryFile lf on lf.id_local = ai.rootFile " +
                                        "left join AgLibraryFolder lfo on lfo.id_local = lf.folder " +
                                        "left join AgLibraryRootFolder rf on rf.id_local = rootFolder " +
                                        "where ai.MasterImage is null " +
                                        (ThisRunType == ReviewType.NewErrors ? "  and lvi.ImageID is null " : string.Empty);
                    dataAdp = new System.Data.SQLite.SQLiteDataAdapter((System.Data.SQLite.SQLiteCommand)cmd);
                    Dt2 = new System.Data.DataTable("ErrorDataTable");
                    dataAdp.Fill(Dt2);
                    NewErrorGrid.ItemsSource = Dt2.DefaultView;
                    string[] visibleColumns = new string[] { "ImageID", "ErrorDetectedDateTime", "ValidationError", "LRImagePath" };
                    foreach (System.Windows.Controls.DataGridColumn col in NewErrorGrid.Columns)
                    {
                        foreach (string s in visibleColumns)
                        {
                            if (s == col.Header.ToString())
                            {
                                col.Visibility = System.Windows.Visibility.Visible;
                                break;
                            }
                            col.Visibility = Visibility.Hidden;
                        }
                    }
                }
            }
            catch (Exception e2)
            {
                MessageBox.Show("Unexpected error in Window_Loaded (ReviewErrors); error received = '" + e2.Message + "'.  Correct before proceeding.", "Catalog ", MessageBoxButton.OK, MessageBoxImage.Error);
                throw;
            }
        }

        private void ContextMenuClick_Show(object sender, RoutedEventArgs e)
        {
            ShowImageDetails dlg = new ShowImageDetails();
            dlg.Selected = NewErrorGrid.SelectedItems.Cast<System.Data.DataRowView>().ToList();
            dlg.Owner = this;
            dlg.ShowDialog();
            if (Dt2.Rows.Count == 0)
            {
                this.Close();   // If we delete all the rows, just close up and go home
            }
            MainWindow.UpdateMainWindowInfo();   // Start an async update on the main window display to reflect what we just changed
        }

        private void ContextMenuClick_Ignore(object sender, RoutedEventArgs e) // Called from form by click uses selected items
        {
            try
            {
                // Because it otherwise complains the collection has been modified and will not enumerate, we copy the list to a variable and iterate over it.
                // There's probably some way to cast this assignment instead of iterate to create the list but this works...
                List<System.Data.DataRowView> l = NewErrorGrid.SelectedItems.Cast<System.Data.DataRowView>().ToList();
                MessageBoxResult result = MessageBox.Show("Are you sure you wish to remove " + l.Count.ToString() + " items?  This means they will go back to being 'new' and be evaluated again the next time you do a find-new.", "Delete Confirm", MessageBoxButton.OKCancel, MessageBoxImage.Question);
                if (result == MessageBoxResult.OK)
                {
                    MarkItemsToIgnore(l); 
                }
                if (Dt2.Rows.Count == 0)
                {
                    this.Close();   // If we delete all the rows, just close up and go home
                }
                MainWindow.UpdateMainWindowInfo();   // Start an async update on the main window display to reflect what we just changed
            }
            catch (Exception e2)
            {
                MessageBox.Show("Unexpected error in ContextMenuClick_Ignore (ReviewErrors); error received = '" + e2.Message + "'.  Correct before proceeding.", "Unexpected Error ", MessageBoxButton.OK, MessageBoxImage.Error);
                throw;
            }
        }

        private void ContextMenuClick_Accept(object sender, RoutedEventArgs e)
        {
            try
            {
                List<System.Data.DataRowView> l = NewErrorGrid.SelectedItems.Cast<System.Data.DataRowView>().ToList();
                for (int i = 0; i < l.Count; i++)
                {
                    if (l[i].Row.ItemArray[2].ToString().Equals(string.Empty))
                    {
                        MessageBox.Show("To use this option you must select only items with invalid checksums", "No checksum Errors", MessageBoxButton.OK, MessageBoxImage.Asterisk);
                        return;
                    }
                }
                MessageBoxResult result = MessageBox.Show("Are you sure you wish to accept " + l.Count.ToString() + " items?  This means their latest checksum will be considered valid and will replace the current one.", "Accept Confirm", MessageBoxButton.OKCancel, MessageBoxImage.Question);
                if (result == MessageBoxResult.OK)
                {
                    MarkItemsToAccept(l); 
                }
                if (Dt2.Rows.Count == 0)
                {
                    this.Close();   // If we delete all the rows, just close up and go home
                }
                MainWindow.UpdateMainWindowInfo();   // Start an async update on the main window display to reflect what we just changed
            }
            catch (Exception e2)
            {
                MessageBox.Show("Unexpected error in ContextMenuClick_Ignore (ReviewErrors); error received = '" + e2.Message + "'.  Correct before proceeding.", "Catalog ", MessageBoxButton.OK, MessageBoxImage.Error);
                throw;
            }
        }
    }
}
