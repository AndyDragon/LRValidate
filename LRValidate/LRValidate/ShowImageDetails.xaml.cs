//  Copyright (c) LE Ferguson, LLC 2013 
namespace LRValidate
{
    // Show one image at a time from a selected list and associated information
    // Allow user to page through the selection 
    // Allow user to mark images to ignore/accept
    using System;
    using System.Collections.Generic;
    using System.ComponentModel;
    using System.IO;
    using System.Windows;
    using System.Windows.Documents;
    using System.Windows.Media.Imaging; 
    
    public partial class ShowImageDetails : Window
    {
        public List<System.Data.DataRowView> Selected;                  // This is set by the caller to those which are selected at the time a show is requested
        private int currentInstance = 0;  // This is 0 based            // Which of the selected items is currently shown
        private BackgroundWorker bgw; // Most recent worker thread      // File load worker thread
        private FindPreviewForImage preview;                                    // Instance of Preview find that is used for display - this is NOT done on a background thread

        public ShowImageDetails()
        {
            InitializeComponent();
        }

        private void Window_Loaded(object sender, RoutedEventArgs e2)
        {
            // This runs after initialization and kicks off processing in several ways and refreshes the screen
            var obj = this.Owner.Owner as MainWindow;  // This forces us to always nest in the same order
            MainWindow mainWindowObj = obj;
            preview = new FindPreviewForImage(MainWindow.CatalogPath);
            RefreshData();
        }

        private void UpArrow_Click(object sender, RoutedEventArgs e)
        {
            // Move to next image (if there is a next) 
            if (currentInstance + 1 < Selected.Count)
            {
                currentInstance++;
                RefreshData();
            }
        }

        private void DownArrow_Click(object sender, RoutedEventArgs e)
        {
            // Move to prior image (if there is a prior) 
            if (currentInstance > 0)
            {
                currentInstance--;
                RefreshData();
            }
        }

        private void AcceptBtn_or_IgnoreBtn_Click(object sender, RoutedEventArgs e) // Note that the UI handler should only have enabled this if it was valid so no validity checking here
        {
            // User asked for an action (look in sender to see which action) 
            List<System.Data.DataRowView> l = new List<System.Data.DataRowView>();
            l.Add(Selected[currentInstance]);
            var obj = this.Owner as ReviewErrors;
            if ((((System.Windows.Controls.Button)sender).Name == "AcceptBtn") ? obj.MarkItemsToAccept(l) : obj.MarkItemsToIgnore(l))
            {   // try to delete and if so 
                // First delete from our constructed selected array as it is gone from the (underlying but not connected) data table
                Selected.Remove(l[0]);
                currentInstance = currentInstance >= Selected.Count ? --currentInstance : currentInstance; // If we were at the end, back up (otherwise we have implicitly advanced) 
                if (currentInstance < 0)
                {
                    this.Close();  // If we have deleted all we had, simply exit; note that surprisingly this does not happen immediately, we need to exclude the following refresh from this path or it gives an error from having no data.
                }
                else
                {
                    RefreshData();
                }
            }
        }

        private void RefreshData()
        {
            // Put up the current image based on currentInstance, pulled from the selected items pushed into this routine by the caller
            SelectedOfBox.Text = Selected.Count.ToString();
            SelectedItemBox.Text = (currentInstance + 1).ToString();

            // 0 = ImageID                    <<<<  Be sure to update this list in the calling program ReviewErrors also if changed
            // 1 = Error Detected Time
            // 2 = Invalid checksum - null for new images, but also null for revalidate if unreadable
            // 3 = Validation Error Message (english for display)
            // 4 = Suggested action message (english for display)
            // 5 = Image Path per Lightroom (for existing images only, otherwise null) 
            // 6 = image path from last time validated (if previously validated) 
            // 7 = orientation per lightroom (force rotation of bitmap)
            // 8 = Last Validation  Date/Time (null for new images) 
            // 9 = Initial checksum Date/Time (null for new images) 
            ImageIDBox.Text = Selected[currentInstance].Row.ItemArray[0].ToString();
            ImagePathLRBox.Text = Selected[currentInstance].Row.ItemArray[5].ToString();
            ImagePathLastBox.Text = Selected[currentInstance].Row.ItemArray[6] == null ? string.Empty : Selected[currentInstance].Row.ItemArray[6].ToString();
            LastTimeValidatedBox.Text = Selected[currentInstance].Row.ItemArray[8] == null ? string.Empty : Selected[currentInstance].Row.ItemArray[8].ToString().Replace('T', ' ');
            InitialTimeValidatedBox.Text = Selected[currentInstance].Row.ItemArray[9] == null ? string.Empty : Selected[currentInstance].Row.ItemArray[9].ToString().Replace('T', ' ');
            ErrorTimeBox.Text = Selected[currentInstance].Row.ItemArray[1].ToString().Replace('T', ' ');
            ErrorBlock.Text = Selected[currentInstance].Row.ItemArray[3].ToString();
            ActionBlock.Text = Selected[currentInstance].Row.ItemArray[4].ToString();

            AcceptBtn.IsEnabled = (!Selected[currentInstance].Row.ItemArray[5].ToString().Equals(string.Empty) && !Selected[currentInstance].Row.ItemArray[2].ToString().Equals(string.Empty)); // Turn off "Accept" unless it's a checksum error (checksum is in #2) AND the old path is present

            LRImageLoadingLabel.Visibility = System.Windows.Visibility.Visible;
            LRImage.Source = null;
            PreviewCacheLoadingLabel.Visibility = System.Windows.Visibility.Visible;
            PreviewCache.Source = null;

            // Handle the current image path (from lightroom catalog) and display from real image 
            if (bgw != null && bgw.IsBusy)
            {
                bgw.CancelAsync();
            }
            bgw = new BackgroundWorker();
            bgw.WorkerSupportsCancellation = true;
            bgw.DoWork += new DoWorkEventHandler(OriginalImageLoad_DoWork);
            bgw.RunWorkerCompleted += new RunWorkerCompletedEventHandler(OriginalImageLoad_Completed);  // (s, e) =>  
            List<object> arguments = new List<object>();
            if (Selected[currentInstance].Row.ItemArray[5].ToString() == string.Empty)
            {  // If the lightroom path is empty, display the on-disk path 
                arguments.Add(Selected[currentInstance].Row.ItemArray[6].ToString());
                DiskImageLabel.Content = "Image on disk from last validation"; 
            }
            else
            {  // display the lightroom path 
                DiskImageLabel.Content = "Image on disk from LR catalog";
                arguments.Add(Selected[currentInstance].Row.ItemArray[5].ToString());
            }
            arguments.Add((int)LRImage.MaxWidth);
            bgw.RunWorkerAsync(arguments);
            System.Threading.Interlocked.Add(ref MainWindow.FileFetchThreadsRunning, 1);

            // Handle the preview cache display 
            PreviewCache.Source = preview.GetPreview(int.Parse(ImageIDBox.Text), (int)(PreviewCache.MaxWidth * PreviewCache.MaxWidth));
            PreviewCacheLoadingLabel.Visibility = System.Windows.Visibility.Hidden;
        }

        private void OriginalImageLoad_DoWork(object sender, DoWorkEventArgs e) // runs in separate thread, can't touch local variables 
        {
            // Worker routine to load the image from disk from the original file
            try
            {
                BackgroundWorker thisWorker = sender as BackgroundWorker;
                List<object> gl = e.Argument as List<object>; // Pull out the arguments passed in
                string imagePath = (string)gl[0];
                int maxLRImageWidth = (int)gl[1]; 
                BitmapImage nowImage = new BitmapImage();
                using (MemoryStream ms = new MemoryStream())
                {
                    using (var stream = new BufferedStream(File.OpenRead(imagePath), 12000000))
                    {
                        stream.CopyTo(ms);
                    }
                    nowImage.BeginInit();
                    nowImage.StreamSource = ms;
                    ms.Seek(0, SeekOrigin.Begin);  // Without this it works sometimes but not always
                    nowImage.DecodePixelHeight = maxLRImageWidth;
                    nowImage.CacheOption = BitmapCacheOption.OnLoad;
                    nowImage.UriCachePolicy = new System.Net.Cache.RequestCachePolicy(System.Net.Cache.RequestCacheLevel.Default);

                    // In theory we would set NowImage.Rotation here similar to in FindPreviewForImage, but it appears the default codec is auto-rotating the image at times, so it's unpredictable
                    nowImage.EndInit();  // Apparently image loading is async so now we wait a bit
                    int loops = 0;
                    while (nowImage.IsDownloading && !thisWorker.CancellationPending)
                    {   // Is this needed? 
                        loops++; 
                        System.Threading.Thread.Sleep(10); 
                    }  
                    nowImage.Freeze();
                    e.Result = thisWorker.CancellationPending ? null : nowImage;  // If we canceled this one, return nothing
                }
            }
            catch (Exception e2)
            {   // If we fail, return nothing
                Console.Write("OriginalImageLoad_DoWork had an error=" + e2.Message);
                e.Result = null;
            }
        }

        private void OriginalImageLoad_Completed(object sender, RunWorkerCompletedEventArgs e) // runs in main thread on thread completion 
        {
            // Completion routine when a file has completely loaded
            // Put up image and mark the "loading" as hidden
            try
            {
                System.Threading.Interlocked.Add(ref MainWindow.FileFetchThreadsRunning, -1);
                MainWindow.UpdateMainWindowInfo(); 
                BitmapImage bmp = e.Result as BitmapImage;
                BackgroundWorker btest = sender as BackgroundWorker;
                if (btest.Equals(bgw))
                {   // We are the most recent worker?
                    LRImage.Source = bmp;
                    LRImageLoadingLabel.Visibility = System.Windows.Visibility.Hidden;
                }
                bgw.Dispose();
            }
            catch (Exception e2)
            {
                Console.Write("OriginalImageLoad_Completed had an error=" + e2.Message); 
            }
        }
    }
}
